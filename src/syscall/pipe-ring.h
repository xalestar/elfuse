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

#include <stdbool.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>

#include "syscall/internal.h" /* fd_block_state_t */

typedef struct pipe_ring pipe_ring_t;

/* A ring of PIPE_DEF_BUFFERS empty buffers. The caller owns its one reference.
 * Returns NULL with errno set.
 */
pipe_ring_t *pipe_ring_create(void);

/* A pipe the host pipe still carries whole, which pipe_ring_convert can move
 * into a ring later. The caller owns its one reference.
 *
 * Returns NULL when out of memory.
 */
pipe_ring_t *pipe_ring_create_dormant(void);

/* True once the ring holds the pipe's data, which it then always does, and true
 * while pipe_ring_convert can still make it so.
 */
bool pipe_ring_active(const pipe_ring_t *ring);
bool pipe_ring_dormant(const pipe_ring_t *ring);

/* The host pipe is about to reach another process. True when the ring is active
 * and its state file has to go along. A dormant ring is left a host pipe for
 * good: the other process could not be told of a later conversion.
 */
bool pipe_ring_leaves_process(pipe_ring_t *ring);

/* Move a dormant ring's pipe into packet mode. wr_fd is the host pipe's write
 * end. rd_fd is its read end, whose queued bytes become the ring's first
 * buffers, or -1 when the caller knows the pipe is empty. A ring that is not
 * dormant any more, or one the host refuses a state file for, stays as it is.
 */
void pipe_ring_convert(pipe_ring_t *ring, int rd_fd, int wr_fd);

/* A host transfer on a pipe whose ring is dormant runs between an enter that
 * returned true and a leave, which keeps pipe_ring_convert from draining the
 * host pipe under it. epoch is pipe_ring_epoch as it read when the caller
 * classified the fd. False means a conversion ran or is running since: wait for
 * pipe_ring_settle, classify the fd again, and transfer on what it is now.
 */
uint32_t pipe_ring_epoch(void);
bool pipe_ring_stream_enter(uint32_t epoch);
void pipe_ring_stream_leave(void);
void pipe_ring_settle(void);

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

/* FIONREAD: the unread bytes in every buffer. 0 or a negative Linux errno. */
int64_t pipe_ring_queued(pipe_ring_t *ring, int32_t *bytes);

/* F_GETPIPE_SZ and F_SETPIPE_SZ. Each returns the ring's capacity in bytes or a
 * negative Linux errno. host_fd is either end of the host pipe.
 */
int64_t pipe_ring_get_size(pipe_ring_t *ring);
int64_t pipe_ring_set_size(pipe_ring_t *ring, int host_fd, unsigned int arg);

/* pipe_full: every buffer is in use. A ring that cannot be read is full. */
bool pipe_ring_full(pipe_ring_t *ring);

/* Replace what fstat on the host pipe said with what Linux says of a pipe. */
void pipe_ring_stat(const pipe_ring_t *ring, struct stat *st);
