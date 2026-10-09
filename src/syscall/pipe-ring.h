/*
 * Pipe buffer ring for packet mode
 *
 * Copyright 2026 elfuse contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * A pipe written with O_DIRECT keeps the boundary of every write, which a host
 * pipe cannot. Such a pipe keeps its host pipe for readiness and hangup only
 * and carries its data in the buffer ring of fs/pipe.c, held in an anonymous
 * file that every process holding the pipe shares.
 *
 * The host pipe holds one byte for each ring buffer in use and is filled to its
 * capacity while every buffer is in use, so host POLLIN, POLLOUT, end of file
 * and EPIPE report what pipe_poll, pipe_read and pipe_write report.
 */

#pragma once

#include <stdint.h>
#include <sys/types.h>
#include <sys/uio.h>

#include "syscall/internal.h" /* fd_block_state_t */

typedef struct pipe_ring pipe_ring_t;

/* A ring of PIPE_DEF_BUFFERS empty buffers. The caller owns its one reference.
 * Returns NULL with errno set.
 */
pipe_ring_t *pipe_ring_create(void);

/* The ring in a state file another elfuse process created. Owns state_fd from
 * here on, and closes it when it returns NULL with errno set.
 */
pipe_ring_t *pipe_ring_adopt(int state_fd);

/* The state file, which is what a forked child adopts. Stays the ring's. */
int pipe_ring_state_fd(const pipe_ring_t *ring);

/* A further reference. The caller already holds one, or holds fd_lock over a
 * slot that does.
 */
void pipe_ring_ref(pipe_ring_t *ring);
void pipe_ring_release(pipe_ring_t *ring);

/* pipe_read (POLLIN) or pipe_write (POLLOUT) on the ring, with host_fd the host
 * pipe end the guest fd names. The contract is io_xfer's: 0 with *out set to
 * the count or to -1 with errno live, or a negative Linux errno when nothing
 * moved and iov is untouched.
 */
int64_t pipe_ring_xfer(pipe_ring_t *ring,
                       int host_fd,
                       short events,
                       struct iovec *iov,
                       int iovcnt,
                       ssize_t *out,
                       const fd_block_state_t *st);
