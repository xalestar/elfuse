/*
 * Test kill(pid, sig) aimed at the caller's parent
 *
 * Copyright 2026 elfuse contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * A forked child probes its parent with kill(getppid(), 0) and then signals it
 * with SIGUSR1. The parent is not in the child's own descendant table, so both
 * calls exercise the single-pid lookup through the fork-family registry.
 */

#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static volatile sig_atomic_t got_usr1 = 0;

static void usr1_handler(int sig)
{
    (void) sig;
    got_usr1 = 1;
}

static bool wait_flag(int max_ms)
{
    for (int i = 0; i < max_ms && !got_usr1; i++) {
        struct timespec ts = {0, 1000000}; /* 1 ms */
        nanosleep(&ts, NULL);
    }
    return got_usr1 != 0;
}

int main(void)
{
    int failed = 0;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = usr1_handler;
    sigaction(SIGUSR1, &sa, NULL);

    pid_t pid = fork();
    if (pid < 0)
        return 1;
    if (pid == 0) {
        if (kill(getppid(), 0) != 0)
            _exit(1);
        if (kill(getppid(), SIGUSR1) != 0)
            _exit(2);
        _exit(0);
    }

    if (!wait_flag(2000)) {
        fprintf(stderr, "FAIL: child kill(getppid(), SIGUSR1) not delivered\n");
        failed++;
    }
    int status = 0;
    if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status)) {
        fprintf(stderr, "FAIL: child did not exit cleanly\n");
        failed++;
    } else if (WEXITSTATUS(status) == 1) {
        fprintf(stderr, "FAIL: kill(getppid(), 0) failed in child\n");
        failed++;
    } else if (WEXITSTATUS(status) == 2) {
        fprintf(stderr, "FAIL: kill(getppid(), SIGUSR1) failed in child\n");
        failed++;
    }

    printf("%s: %d failed\n", failed == 0 ? "PASS" : "FAIL", failed);
    return failed == 0 ? 0 : 1;
}
