/*****************************************************************************\
 *  util_exec.c - shared fork/exec helpers for slurmbrokerd modules.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See util_exec.h for the
 *  contract.
 *
 *  This is the single source of truth for "wait for a child process up
 *  to N seconds, otherwise SIGKILL and reap". Before M11 it was
 *  duplicated as a static helper in stage.c, handler_remote.c and
 *  rewrite.c; M11 collapses them into this one definition.
\*****************************************************************************/

#include "config.h"

#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "src/common/log.h"

#include "util_exec.h"

#define POLL_STEP_US  (100 * 1000)  /* 100ms */

int brokerd_waitpid_timeout(pid_t pid, int timeout_s)
{
	int wstat = 0;
	int slept_us = 0;
	const int budget_us = timeout_s * 1000 * 1000;

	while (slept_us < budget_us) {
		pid_t r = waitpid(pid, &wstat, WNOHANG);

		if (r == pid)
			break;
		if (r < 0 && errno != EINTR) {
			error("brokerd_waitpid_timeout: waitpid: %m");
			return -1;
		}
		usleep(POLL_STEP_US);
		slept_us += POLL_STEP_US;
	}

	if (slept_us >= budget_us) {
		warning("brokerd_waitpid_timeout: child pid=%d exceeded %ds timeout, sending SIGKILL",
		        (int) pid, timeout_s);
		(void) kill(pid, SIGKILL);
		(void) waitpid(pid, &wstat, 0);
		return -1;
	}

	if (!WIFEXITED(wstat)) {
		debug("brokerd_waitpid_timeout: child pid=%d killed by signal",
		      (int) pid);
		return -1;
	}
	return WEXITSTATUS(wstat);
}
