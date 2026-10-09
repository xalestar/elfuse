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
 */

#pragma once

typedef struct pipe_ring pipe_ring_t;

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
