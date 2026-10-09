/*
 * Pipe buffer ring for packet mode
 *
 * Copyright 2026 elfuse contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <unistd.h>

#include "utils.h"

#include "syscall/pipe-ring.h"

struct pipe_ring {
    _Atomic unsigned int refs;
    int state_fd;
};

/* Excludes the threads of this process from every ring. An fcntl lock on the
 * state file excludes other processes only, and closing any descriptor of that
 * file drops it (fcntl(2)), so every close of one runs under this lock.
 */
static pthread_mutex_t pipe_ring_lock = PTHREAD_MUTEX_INITIALIZER;

pipe_ring_t *pipe_ring_adopt(int state_fd)
{
    pipe_ring_t *ring = NULL;
    if (fd_set_cloexec(state_fd) == 0)
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
    return ring;
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
