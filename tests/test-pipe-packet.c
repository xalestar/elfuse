/*
 * Packet mode on a pipe (O_DIRECT)
 *
 * Copyright 2026 elfuse contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Every expected value is what Linux 6.12 answers. The host pipe is a byte
 * stream, so each of these fails wherever elfuse lets it carry the data.
 *
 * Syscalls exercised: pipe2(59), read(63), write(64), readv(65), writev(66),
 *                     fcntl(25), dup(23), close(57), clone(220), wait4(260),
 *                     rt_sigaction(134)
 */

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>

#include "test-harness.h"

int passes = 0, fails = 0;

#define PAGE 4096
#define SLOTS 16 /* PIPE_DEF_BUFFERS */

static char scratch[1 << 20];
static volatile sig_atomic_t sigpipes;

static void on_sigpipe(int signo)
{
    (void) signo;
    sigpipes++;
}

/* The count, or the negated errno. */
static long rd(int fd, size_t n)
{
    long r = read(fd, scratch, n);
    return r < 0 ? -errno : r;
}

static long wr(int fd, size_t n)
{
    static char src[1 << 20];
    long r = write(fd, src, n);
    return r < 0 ? -errno : r;
}

static void set_fl(int fd, int set, int clear)
{
    fcntl(fd, F_SETFL, (fcntl(fd, F_GETFL) | set) & ~clear);
}

static void packet_pipe(int p[2])
{
    if (pipe2(p, O_DIRECT) != 0) {
        printf("pipe2(O_DIRECT) failed: errno=%d\n", errno);
        _exit(1);
    }
    set_fl(p[0], O_NONBLOCK, 0);
}

static void close_pair(int p[2])
{
    close(p[0]);
    close(p[1]);
}

static void *close_later(void *arg)
{
    usleep(100000);
    close(*(int *) arg);
    return NULL;
}

static void *read_later(void *arg)
{
    char buf[PAGE];
    usleep(100000);
    return (void *) read(*(int *) arg, buf, sizeof(buf));
}

static void test_boundaries(void)
{
    int p[2];
    packet_pipe(p);

    wr(p[1], 2);
    wr(p[1], 3);
    TEST("each write is one read");
    EXPECT_TRUE(
        rd(p[0], 100) == 2 && rd(p[0], 100) == 3 && rd(p[0], 100) == -EAGAIN,
        "two writes did not come back as two reads");

    wr(p[1], 6);
    TEST("a short read drops the rest");
    EXPECT_TRUE(rd(p[0], 2) == 2 && rd(p[0], 100) == -EAGAIN,
                "the tail of a packet survived a short read");

    TEST("a write above a page is split");
    EXPECT_TRUE(wr(p[1], PAGE + 904) == PAGE + 904 && rd(p[0], 65536) == PAGE &&
                    rd(p[0], 65536) == 904,
                "5000 bytes did not arrive as 4096 and 904");

    TEST("an empty write queues nothing");
    EXPECT_TRUE(wr(p[1], 0) == 0 && rd(p[0], 100) == -EAGAIN,
                "write of 0 left something to read");

    struct iovec out[2] = {{"ab", 2}, {"cde", 3}};
    char first[1], rest[16];
    struct iovec in[2] = {{first, 1}, {rest, sizeof(rest)}};
    TEST("writev is one packet");
    EXPECT_TRUE(writev(p[1], out, 2) == 5 && readv(p[0], in, 2) == 5 &&
                    first[0] == 'a' && !memcmp(rest, "bcde", 4),
                "writev and readv disagree about the packet");

    TEST("the wrong end is EBADF");
    EXPECT_TRUE(rd(p[1], 1) == -EBADF && wr(p[0], 1) == -EBADF,
                "an end accepted the other end's operation");
    close_pair(p);
}

static void test_hangup(void)
{
    int p[2];
    packet_pipe(p);
    wr(p[1], 3);
    wr(p[1], 2);
    close(p[1]);
    TEST("end of file after the packets");
    EXPECT_TRUE(rd(p[0], 100) == 3 && rd(p[0], 100) == 2 &&
                    rd(p[0], 100) == 0 && rd(p[0], 100) == 0,
                "the queued packets or the end of file went missing");
    close(p[0]);

    packet_pipe(p);
    set_fl(p[0], 0, O_NONBLOCK);
    pthread_t t;
    pthread_create(&t, NULL, close_later, &p[1]);
    TEST("a blocked read sees the close");
    EXPECT_EQ(rd(p[0], 100), 0, "the read did not return 0");
    pthread_join(t, NULL);
    close(p[0]);

    packet_pipe(p);
    close(p[0]);
    sigpipes = 0;
    TEST("no reader is EPIPE and SIGPIPE");
    EXPECT_TRUE(wr(p[1], 2) == -EPIPE && sigpipes == 1,
                "the write did not report its lost reader");
    close(p[1]);
}

static void test_full(void)
{
    int p[2];
    packet_pipe(p);
    set_fl(p[1], O_NONBLOCK, 0);

    int packets = 0;
    while (wr(p[1], 1) == 1)
        packets++;
    TEST("16 packets of any size fill it");
    EXPECT_TRUE(packets == SLOTS && wr(p[1], 1) == -EAGAIN,
                "one-byte packets did not stop at 16");

    rd(p[0], 100);
    TEST("a write returns what fit");
    EXPECT_EQ(wr(p[1], PAGE + 904), PAGE, "the second packet was not refused");

    /* Still full. A blocking write has to wait for the reader thread. */
    set_fl(p[1], 0, O_NONBLOCK);
    pthread_t t;
    void *got;
    pthread_create(&t, NULL, read_later, &p[0]);
    long written = wr(p[1], 7);
    pthread_join(t, &got);
    TEST("a blocking write waits");
    EXPECT_TRUE(written == 7 && (long) got == 1,
                "the write did not complete behind one read");
    close_pair(p);
}

static void test_fork(void)
{
    int p[2];
    packet_pipe(p);
    wr(p[1], 1);
    wr(p[1], 2);
    pid_t pid = fork();
    if (pid == 0) {
        /* Takes the first packet, adds one, and leaves the middle one. */
        long got = rd(p[0], 100);
        _exit(got == 1 && wr(p[1], 3) == 3 ? 0 : 1);
    }
    int status = 0;
    waitpid(pid, &status, 0);
    TEST("a child shares the packets");
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0 &&
                    rd(p[0], 100) == 2 && rd(p[0], 100) == 3 &&
                    rd(p[0], 100) == -EAGAIN,
                "parent and child did not see one queue");
    close_pair(p);
}

static void test_mode_is_per_description(void)
{
    int p[2];
    packet_pipe(p);
    int alias = dup(p[1]);
    wr(alias, 2);
    wr(alias, 2);
    TEST("a dup writes packets too");
    EXPECT_TRUE(rd(p[0], 100) == 2 && rd(p[0], 100) == 2,
                "the alias wrote a stream");

    /* Cleared through the alias, so the original stops writing packets. Stream
     * writes share a buffer; the packet ahead of them keeps its boundary.
     */
    wr(p[1], 2);
    set_fl(alias, 0, O_DIRECT);
    wr(p[1], 2);
    wr(p[1], 3);
    TEST("clearing O_DIRECT ends packets");
    EXPECT_TRUE(
        rd(p[0], 100) == 2 && rd(p[0], 100) == 5 && rd(p[0], 100) == -EAGAIN,
        "the stream writes kept boundaries, or the packet lost its");
    close(alias);
    close_pair(p);
}

int main(void)
{
    signal(SIGPIPE, on_sigpipe);
    printf("test-pipe-packet:\n");

    test_boundaries();
    test_hangup();
    test_full();
    test_fork();
    test_mode_is_per_description();

    SUMMARY("test-pipe-packet");
    return fails > 0 ? 1 : 0;
}
