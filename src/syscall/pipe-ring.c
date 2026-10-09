/*
 * Pipe buffer ring for packet mode
 *
 * Copyright 2026 elfuse contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include "utils.h"

#include "runtime/thread.h"
#include "syscall/abi.h"
#include "syscall/io.h"
#include "syscall/pipe-ring.h"
#include "syscall/signal.h"

/* Linux PAGE_SIZE as the guest sees it: the most one pipe_buffer holds. */
#define RING_PAGE 4096U

/* PIPE_DEF_BUFFERS, and fs.pipe-max-size (1 MiB) in pages. */
#define RING_DEF_SLOTS 16U
#define RING_MAX_SLOTS 256U

#define RING_MAGIC UINT32_C(0x52504645)
#define RING_DATA_OFF 4096

/* The most a host pipe holds: BIG_PIPE_SIZE, xnu bsd/sys/pipe.h. */
#define RING_HOST_PIPE_MAX 65536

/* PIPE_BUF_FLAG_PACKET and PIPE_BUF_FLAG_CAN_MERGE, include/linux/pipe_fs_i.h.
 */
#define RING_BUF_PACKET 1U
#define RING_BUF_CAN_MERGE 2U

typedef struct {
    uint16_t off; /* first unread byte of the page */
    uint16_t len; /* unread bytes */
    uint32_t flags;
} ring_buf_t;

/* The state file: this at offset 0, then one page for each buffer. */
typedef struct {
    uint32_t magic;
    uint32_t slots;      /* buffers in the ring, a power of two */
    uint32_t head, tail; /* buffers written and consumed so far */
    ring_buf_t buf[RING_MAX_SLOTS];
} ring_meta_t;

_Static_assert(sizeof(ring_meta_t) <= RING_DATA_OFF,
               "ring metadata runs into the first data page");

struct pipe_ring {
    _Atomic unsigned int refs;
    int state_fd;
    ino_t ino;
};

/* Excludes the threads of this process from every ring. The fcntl lock in
 * ring_lock excludes other processes only, and closing any descriptor of a
 * state file drops it (fcntl(2)), so pipe_ring_release closes under this lock.
 */
static pthread_mutex_t pipe_ring_lock = PTHREAD_MUTEX_INITIALIZER;

static const uint8_t ring_filler[RING_HOST_PIPE_MAX];

static off_t ring_data_off(uint32_t idx)
{
    return RING_DATA_OFF + (off_t) idx * RING_PAGE;
}

static int ring_setlk(int fd, short type)
{
    struct flock fl = {.l_type = type, .l_whence = SEEK_SET};
    return fcntl(fd, F_SETLK, &fl);
}

/* Returns 0 holding the ring, or a negative Linux errno holding nothing. The
 * holder it waits out is another process inside a handful of host calls, so the
 * wait yields rather than parks; only an execve reaping this thread ends it.
 */
static int64_t ring_lock(pipe_ring_t *ring)
{
    for (unsigned tries = 0;; tries++) {
        pthread_mutex_lock(&pipe_ring_lock);
        if (ring_setlk(ring->state_fd, F_WRLCK) == 0)
            return 0;
        int saved_errno = errno;
        pthread_mutex_unlock(&pipe_ring_lock);

        if (saved_errno != EAGAIN && saved_errno != EACCES) {
            errno = saved_errno;
            return linux_errno();
        }
        if (thread_stop_requested())
            return -LINUX_EINTR;
        if (tries < 64)
            sched_yield();
        else
            usleep(100);
    }
}

static void ring_unlock(pipe_ring_t *ring)
{
    ring_setlk(ring->state_fd, F_UNLCK);
    pthread_mutex_unlock(&pipe_ring_lock);
}

/* The state file is elfuse's own, but its bytes index into this process's
 * stack, so nothing in it is trusted further than the checks here.
 */
static bool ring_load(const pipe_ring_t *ring, ring_meta_t *m)
{
    errno = EIO;
    if (pread(ring->state_fd, m, sizeof(*m), 0) != (ssize_t) sizeof(*m))
        return false;

    uint32_t used = m->head - m->tail;
    if (m->magic != RING_MAGIC || m->slots == 0 || m->slots > RING_MAX_SLOTS ||
        (m->slots & (m->slots - 1)) != 0 || used > m->slots)
        return false;
    for (uint32_t i = 0; i < used; i++) {
        const ring_buf_t *b = &m->buf[(m->tail + i) & (m->slots - 1)];
        if ((uint32_t) b->off + b->len > RING_PAGE)
            return false;
    }
    return true;
}

static bool ring_store(const pipe_ring_t *ring, const ring_meta_t *m)
{
    if (pwrite(ring->state_fd, m, sizeof(*m), 0) == (ssize_t) sizeof(*m))
        return true;
    errno = EIO;
    return false;
}

/* Put `added` tokens in the host pipe for the buffers a write just took, and
 * fill it when the ring is now full. Tokens go in before the metadata that
 * accounts for them and come out after it (ring_tokens_trim), so a process
 * killed between the two leaves the host pipe over-reporting, which the next
 * reader corrects, and never under-reporting, which nothing would.
 */
static void ring_tokens_add(int wr_fd, uint32_t added, bool full)
{
    if (added > 0)
        (void) write(wr_fd, ring_filler, added);
    while (full && write(wr_fd, ring_filler, sizeof(ring_filler)) > 0) {
    }
}

/* Leave one token for each buffer in use. A full ring keeps its filler. The
 * caller holds pipe_ring_lock, which is what makes one sink enough.
 */
static void ring_tokens_trim(int rd_fd, uint32_t used, bool full)
{
    static uint8_t sink[RING_HOST_PIPE_MAX];
    int queued = 0;
    if (full || ioctl(rd_fd, FIONREAD, &queued) < 0)
        return;
    while (queued > 0 && (uint32_t) queued > used) {
        size_t excess = (uint32_t) queued - used;
        ssize_t n =
            read(rd_fd, sink, excess < sizeof(sink) ? excess : sizeof(sink));
        if (n <= 0)
            return;
        queued -= (int) n;
    }
}

/* One read or write in progress. */
typedef struct {
    pipe_ring_t *ring;
    int host_fd;
    struct iovec *iov;
    int iovcnt;
    uint64_t want;      /* bytes still asked for */
    ssize_t done;       /* bytes moved */
    uint32_t buf_flags; /* what a buffer this write adds is */
} ring_xfer_t;

/* Move n bytes between page and the front of the vector, which loses them. The
 * caller has n within both.
 */
static void ring_iov_move(ring_xfer_t *x, uint8_t *page, size_t n, bool fill)
{
    while (n > 0 && x->iovcnt > 0) {
        struct iovec *v = x->iov;
        if (v->iov_len == 0) {
            x->iov++;
            x->iovcnt--;
            continue;
        }
        size_t take = v->iov_len < n ? v->iov_len : n;
        if (fill)
            memcpy(v->iov_base, page, take);
        else
            memcpy(page, v->iov_base, take);
        v->iov_base = (char *) v->iov_base + take;
        v->iov_len -= take;
        page += take;
        n -= take;
    }
}

/* Take n bytes of the write into the page of buffer idx, at offset at. */
static bool ring_put(ring_xfer_t *x, uint32_t idx, uint32_t at, size_t n)
{
    uint8_t page[RING_PAGE];
    ring_iov_move(x, page, n, false);
    if (pwrite(x->ring->state_fd, page, n, ring_data_off(idx) + at) !=
        (ssize_t) n) {
        errno = EIO;
        return false;
    }
    x->done += (ssize_t) n;
    x->want -= n;
    return true;
}

/* One pass of pipe_write with the ring held.
 *
 * Returns 0 when the write is complete, 1 when the ring filled first, -1 with
 * errno set.
 */
static int ring_write_pass(ring_xfer_t *x, bool first)
{
    ring_meta_t m;
    if (!ring_load(x->ring, &m))
        return -1;

    /* pipe_write looks at pipe->readers before it writes and again after each
     * wait. An empty host write reports the same: EPIPE once the last reader is
     * gone, and EBADF on a read end.
     */
    if (write(x->host_fd, ring_filler, 0) < 0)
        return -1;

    uint32_t mask = m.slots - 1, added = 0;
    bool dirty = false;

    /* The part of a write that does not fill a page joins the last buffer when
     * that buffer takes merges and has the room. pipe_write does this ahead of
     * its loop and without asking what mode the write is in, so a packet write
     * behind stream data loses its boundary.
     */
    size_t chars = x->want & (RING_PAGE - 1);
    if (first && chars > 0 && m.head != m.tail) {
        uint32_t idx = (m.head - 1) & mask;
        ring_buf_t *b = &m.buf[idx];
        uint32_t end = (uint32_t) b->off + b->len;
        if ((b->flags & RING_BUF_CAN_MERGE) && end + chars <= RING_PAGE) {
            if (!ring_put(x, idx, end, chars))
                return -1;
            b->len = (uint16_t) (b->len + chars);
            dirty = true;
        }
    }

    while (x->want > 0 && m.head - m.tail < m.slots) {
        size_t n = x->want < RING_PAGE ? x->want : RING_PAGE;
        uint32_t idx = m.head & mask;
        if (!ring_put(x, idx, 0, n))
            return -1;
        m.buf[idx] = (ring_buf_t) {.len = (uint16_t) n, .flags = x->buf_flags};
        m.head++;
        added++;
        dirty = true;
    }

    ring_tokens_add(x->host_fd, added, m.head - m.tail == m.slots);
    if (dirty && !ring_store(x->ring, &m))
        return -1;
    return x->want > 0 ? 1 : 0;
}

/* One pass of pipe_read with the ring held.
 *
 * Returns 0 when the read is over (bytes moved, or end of file with none), 1
 * when the ring is empty and a writer is left, -1 with errno set.
 */
static int ring_read_pass(ring_xfer_t *x)
{
    ring_meta_t m;
    if (!ring_load(x->ring, &m))
        return -1;

    uint8_t page[RING_PAGE];
    uint32_t mask = m.slots - 1;
    bool dirty = false;
    while (x->want > 0 && m.head != m.tail) {
        uint32_t idx = m.tail & mask;
        ring_buf_t *b = &m.buf[idx];
        size_t n = b->len < x->want ? b->len : x->want;
        if (pread(x->ring->state_fd, page, n, ring_data_off(idx) + b->off) !=
            (ssize_t) n) {
            errno = EIO;
            return -1;
        }
        ring_iov_move(x, page, n, true);
        b->off = (uint16_t) (b->off + n);
        b->len = (uint16_t) (b->len - n);
        x->done += (ssize_t) n;
        x->want -= n;
        dirty = true;

        /* A packet ends the read, and what the caller did not take of it is
         * gone (pipe_read, PIPE_BUF_FLAG_PACKET).
         */
        if (b->flags & RING_BUF_PACKET) {
            b->len = 0;
            x->want = 0;
        }
        if (b->len == 0)
            m.tail++;
    }

    if (dirty && !ring_store(x->ring, &m))
        return -1;
    uint32_t used = m.head - m.tail;
    ring_tokens_trim(x->host_fd, used, used == m.slots);
    if (x->done > 0)
        return 0;

    /* Nothing in the ring, so nothing but a stale token in the host pipe: its
     * own read then tells a write side that is closed (0) from one that is open
     * (EAGAIN).
     */
    uint8_t token;
    ssize_t n;
    while ((n = read(x->host_fd, &token, 1)) > 0) {
    }
    if (n == 0)
        return 0;
    return errno == EAGAIN ? 1 : -1;
}

int64_t pipe_ring_xfer(pipe_ring_t *ring,
                       int host_fd,
                       short events,
                       struct iovec *iov,
                       int iovcnt,
                       ssize_t *out,
                       const fd_block_state_t *st)
{
    bool is_read = (events & POLLIN) != 0;
    ring_xfer_t x = {
        .ring = ring,
        .host_fd = host_fd,
        .iov = iov,
        .iovcnt = iovcnt,
        .buf_flags = st->guest_direct ? RING_BUF_PACKET : RING_BUF_CAN_MERGE,
    };
    for (int i = 0; i < iovcnt; i++) {
        if (!iov_total_add(x.want, iov[i].iov_len, &x.want))
            return -LINUX_EINVAL;
    }
    if (x.want == 0) {
        *out = 0;
        return 0;
    }

    /* The ring would serve a read through the write end. The host pipe knows
     * which end this is; the ring does not.
     */
    if (is_read) {
        int fl = fcntl(host_fd, F_GETFL);
        if (fl < 0 || (fl & O_ACCMODE) == O_WRONLY) {
            *out = -1;
            errno = EBADF;
            return 0;
        }
    }

    int err = 0;
    int64_t fail = 0;
    for (bool first = true;; first = false) {
        fail = ring_lock(ring);
        if (fail < 0)
            break;
        int rc = is_read ? ring_read_pass(&x) : ring_write_pass(&x, first);
        int pass_errno = errno;
        ring_unlock(ring);

        if (rc == 0)
            break;
        if (rc < 0) {
            err = pass_errno;
            break;
        }
        if (st->guest_nonblock) {
            err = EAGAIN;
            break;
        }
        fail = io_wait_fd_or_interrupted(host_fd, events);
        if (fail < 0)
            break;
    }

    /* pipe_write signals a writer that lost its reader even when it has a count
     * to return, the same case io_xfer answers for a host pipe.
     */
    if (!is_read && x.done > 0 && err == EPIPE)
        signal_queue(LINUX_SIGPIPE);

    if (x.done == 0 && fail < 0)
        return fail;
    if (x.done == 0 && err != 0) {
        *out = -1;
        errno = err;
        return 0;
    }
    *out = x.done;
    return 0;
}

int64_t pipe_ring_queued(pipe_ring_t *ring, int32_t *bytes)
{
    int64_t rc = ring_lock(ring);
    if (rc < 0)
        return rc;
    ring_meta_t m;
    bool ok = ring_load(ring, &m);
    ring_unlock(ring);
    if (!ok)
        return -LINUX_EIO;

    /* pipe_ioctl sums buf->len over the ring, packets or not. */
    int32_t sum = 0;
    for (uint32_t i = m.tail; i != m.head; i++)
        sum += m.buf[i & (m.slots - 1)].len;
    *bytes = sum;
    return 0;
}

int64_t pipe_ring_get_size(pipe_ring_t *ring)
{
    int64_t rc = ring_lock(ring);
    if (rc < 0)
        return rc;
    ring_meta_t m;
    bool ok = ring_load(ring, &m);
    ring_unlock(ring);
    return ok ? (int64_t) m.slots * RING_PAGE : -LINUX_EIO;
}

/* Move the buffers in use to the front of a ring of `slots`, in order, which is
 * what pipe_resize_ring leaves.
 *
 * Returns false with nothing committed.
 */
static bool ring_resize(const pipe_ring_t *ring,
                        ring_meta_t *m,
                        uint32_t slots,
                        uint8_t *pages)
{
    uint32_t used = m->head - m->tail;
    ring_meta_t next = {.magic = RING_MAGIC, .slots = slots, .head = used};
    for (uint32_t i = 0; i < used; i++) {
        uint32_t idx = (m->tail + i) & (m->slots - 1);
        next.buf[i] = m->buf[idx];
        if (pread(ring->state_fd, pages + (size_t) i * RING_PAGE, RING_PAGE,
                  ring_data_off(idx)) < 0)
            return false;
    }
    for (uint32_t i = 0; i < used; i++) {
        if (pwrite(ring->state_fd, pages + (size_t) i * RING_PAGE, RING_PAGE,
                   ring_data_off(i)) != (ssize_t) RING_PAGE)
            return false;
    }
    if (!ring_store(ring, &next))
        return false;
    *m = next;
    return true;
}

int64_t pipe_ring_set_size(pipe_ring_t *ring, int host_fd, unsigned int arg)
{
    /* round_pipe_size and pipe_set_size, fs/pipe.c. A request above
     * fs.pipe-max-size is refused the way it is for a caller without
     * CAP_SYS_RESOURCE.
     */
    if (arg > (1U << 31))
        return -LINUX_EINVAL;
    uint32_t slots = 1;
    while (slots * RING_PAGE < arg && slots < RING_MAX_SLOTS)
        slots <<= 1;
    if ((uint64_t) slots * RING_PAGE < arg)
        return -LINUX_EPERM;

    uint8_t *pages = malloc((size_t) RING_MAX_SLOTS * RING_PAGE);
    if (!pages)
        return -LINUX_ENOMEM;

    int64_t rc = ring_lock(ring);
    if (rc < 0) {
        free(pages);
        return rc;
    }
    ring_meta_t m;
    if (!ring_load(ring, &m)) {
        rc = -LINUX_EIO;
    } else if (m.head - m.tail > slots) {
        rc = -LINUX_EBUSY; /* pipe_resize_ring */
    } else if (slots != m.slots && !ring_resize(ring, &m, slots, pages)) {
        rc = -LINUX_EIO;
    } else {
        /* Whichever end host_fd is can do one of these and fails the other. The
         * end that cannot is put right by the next transfer on the other side.
         */
        uint32_t used = m.head - m.tail;
        ring_tokens_add(host_fd, 0, used == m.slots);
        ring_tokens_trim(host_fd, used, used == m.slots);
        rc = (int64_t) m.slots * RING_PAGE;
    }
    ring_unlock(ring);
    free(pages);
    return rc;
}

void pipe_ring_stat(const pipe_ring_t *ring, struct stat *st)
{
    /* get_pipe_inode: S_IFIFO | S_IRUSR | S_IWUSR and one inode for both ends,
     * whose size stays 0. The state file's inode number is the one value every
     * process holding the pipe agrees on.
     */
    st->st_mode = S_IFIFO | S_IRUSR | S_IWUSR;
    st->st_ino = ring->ino;
    st->st_nlink = 1;
    st->st_size = 0;
    st->st_blocks = 0;
    st->st_blksize = RING_PAGE;
}

pipe_ring_t *pipe_ring_adopt(int state_fd)
{
    struct stat st;
    pipe_ring_t *ring = NULL;
    if (fstat(state_fd, &st) == 0 && fd_set_cloexec(state_fd) == 0)
        ring = malloc(sizeof(*ring));
    if (!ring) {
        int saved_errno = errno;
        pthread_mutex_lock(&pipe_ring_lock);
        close(state_fd);
        pthread_mutex_unlock(&pipe_ring_lock);
        errno = saved_errno;
        return NULL;
    }
    atomic_init(&ring->refs, 1);
    ring->state_fd = state_fd;
    ring->ino = st.st_ino;
    return ring;
}

pipe_ring_t *pipe_ring_create(void)
{
    int fd = tmpfile_anon("pipe");
    if (fd < 0)
        return NULL;

    /* Not yet shared, so the close needs no lock. */
    ring_meta_t m = {.magic = RING_MAGIC, .slots = RING_DEF_SLOTS};
    if (pwrite(fd, &m, sizeof(m), 0) != (ssize_t) sizeof(m)) {
        close(fd);
        errno = EIO;
        return NULL;
    }
    return pipe_ring_adopt(fd);
}

int pipe_ring_state_fd(const pipe_ring_t *ring)
{
    return ring->state_fd;
}

void pipe_ring_ref(pipe_ring_t *ring)
{
    atomic_fetch_add_explicit(&ring->refs, 1, memory_order_relaxed);
}

void pipe_ring_release(pipe_ring_t *ring)
{
    /* Release, then acquire before the free: the pairing fd_lifetime_release
     * uses, for the same reason.
     */
    if (atomic_fetch_sub_explicit(&ring->refs, 1, memory_order_release) != 1)
        return;
    atomic_thread_fence(memory_order_acquire);

    /* Callers release on a failure path and read errno afterwards. */
    int saved_errno = errno;
    pthread_mutex_lock(&pipe_ring_lock);
    close(ring->state_fd);
    pthread_mutex_unlock(&pipe_ring_lock);
    free(ring);
    errno = saved_errno;
}
