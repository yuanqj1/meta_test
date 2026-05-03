/*****************************************************************************\
 *  util_exec.h - shared fork/exec helpers for slurmbrokerd modules.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/checklists/M11-software.md
 *  §6.M11-T1 for context.
 *
 *  Three other broker modules (stage.c, handler_remote.c, rewrite.c) all
 *  fork child processes (rsync / sudo sbatch / sudo mkdir / sudo rm /
 *  sudo du / lookup_software.sh) and need a portable
 *  "waitpid-with-timeout-then-SIGKILL" primitive. M11 collects that one
 *  duplicated function into this shared helper module so any future
 *  fork+exec call site has one definition to use.
 *
 *  Slurm-version independence
 *  --------------------------
 *  Pure POSIX (waitpid + WNOHANG + kill + nanosleep equivalents). No
 *  libslurm RPCs.
\*****************************************************************************/

#ifndef _BROKERD_UTIL_EXEC_H
#define _BROKERD_UTIL_EXEC_H

#include <sys/types.h>

/*
 * Wait for `pid` to exit, polling 100ms at a time. After `timeout_s`
 * seconds the child is sent SIGKILL and reaped synchronously.
 *
 * Returns:
 *    >= 0   - the child's exit status (0 means success).
 *    -1     - any failure: timeout (after SIGKILL), waitpid error, or
 *             the child was killed by a signal rather than exiting
 *             normally.
 *
 * Caller is expected to log diagnostics tagged with the relevant
 * trace_id; this helper itself only emits a single warning() on
 * timeout so the operator can tell the difference between "rsync took
 * too long" and "rsync exited with rc=1".
 */
extern int brokerd_waitpid_timeout(pid_t pid, int timeout_s);

#endif /* _BROKERD_UTIL_EXEC_H */
