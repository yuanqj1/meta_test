/*****************************************************************************\
 *  software.c - resolve <cluster, app> to an absolute install path.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See software.h for the
 *  contract and doc/checklists/M11-software.md for the design.
 *
 *  Implementation
 *  --------------
 *    pipe()
 *    fork()
 *      child : dup2 stdout to pipe write end, exec
 *              g_broker_conf.lookup_software_script <cluster> <app>
 *              (stderr left attached to broker's stderr/journal)
 *      parent: select() on pipe read end with timeout = LookupTimeoutSec
 *              read up to PATH_MAX bytes, trim newline
 *              brokerd_waitpid_timeout() to reap with a tight grace
 *              window
 *
 *    Then validate:
 *      - exit code == 0
 *      - stdout non-empty
 *      - first character is '/' (absolute path required)
\*****************************************************************************/

#include "config.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/log.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "broker_conf.h"
#include "proto.h"        /* BROKERD_ERR_LOOKUP_TIMEOUT / FAILED */
#include "software.h"
#include "util_exec.h"

#define LOOKUP_TIMEOUT_DEFAULT_S  10
#define LOOKUP_OUTPUT_MAX         4096  /* PATH_MAX-ish on Linux */

/*
 * Read up to (bufsz - 1) bytes from `fd` waiting at most `timeout_ms`
 * for the FIRST byte. Subsequent bytes are read in non-blocking-ish
 * fashion until EOF or buffer full. Always nul-terminates `buf`.
 *
 * Returns the number of bytes read (0 on clean EOF without data),
 * or -1 with errno set on error / timeout.
 */
static int _read_until_eof_or_timeout(int fd, char *buf, size_t bufsz,
				      int timeout_ms)
{
	struct timeval tv;
	fd_set rfds;
	size_t off = 0;
	int n;

	if (bufsz == 0)
		return 0;
	memset(buf, 0, bufsz);

	tv.tv_sec  = timeout_ms / 1000;
	tv.tv_usec = (timeout_ms % 1000) * 1000;
	FD_ZERO(&rfds);
	FD_SET(fd, &rfds);

	n = select(fd + 1, &rfds, NULL, NULL, &tv);
	if (n == 0) {
		errno = ETIMEDOUT;
		return -1;
	}
	if (n < 0)
		return -1;

	/* Drain whatever is there (the script writes one short line). */
	while (off + 1 < bufsz) {
		ssize_t r = read(fd, buf + off, bufsz - 1 - off);

		if (r > 0) {
			off += (size_t) r;
			continue;
		}
		if (r == 0)
			break; /* EOF */
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			break;
		return -1;
	}
	buf[off] = '\0';
	return (int) off;
}

int lookup_software_path(const char *cluster, const char *app, char **out_path)
{
	int pipefd[2] = { -1, -1 };
	pid_t pid;
	char buf[LOOKUP_OUTPUT_MAX];
	int n, rc, timeout_s;
	char *p, *nl;

	if (out_path)
		*out_path = NULL;

	if (!cluster || !*cluster || !app || !*app) {
		debug("lookup_software: empty cluster/app");
		return BROKERD_ERR_LOOKUP_FAILED;
	}
	if (!g_broker_conf.lookup_software_script ||
	    !*g_broker_conf.lookup_software_script) {
		debug("lookup_software: lookup_software_script not configured");
		return BROKERD_ERR_LOOKUP_FAILED;
	}

	timeout_s = (int) g_broker_conf.lookup_timeout_sec;
	if (timeout_s <= 0)
		timeout_s = LOOKUP_TIMEOUT_DEFAULT_S;

	if (pipe(pipefd) < 0) {
		error("lookup_software: pipe: %m");
		return BROKERD_ERR_LOOKUP_FAILED;
	}

	pid = fork();
	if (pid < 0) {
		error("lookup_software: fork: %m");
		(void) close(pipefd[0]);
		(void) close(pipefd[1]);
		return BROKERD_ERR_LOOKUP_FAILED;
	}
	if (pid == 0) {
		/* child */
		(void) close(pipefd[0]);
		if (pipefd[1] != STDOUT_FILENO) {
			(void) dup2(pipefd[1], STDOUT_FILENO);
			(void) close(pipefd[1]);
		}
		/* leave stderr attached to broker's stderr/syslog so script
		 * diagnostics reach the operator */
		execl(g_broker_conf.lookup_software_script,
		      g_broker_conf.lookup_software_script,
		      cluster, app, (char *) NULL);
		_exit(127);
	}

	/* parent */
	(void) close(pipefd[1]);

	n = _read_until_eof_or_timeout(pipefd[0], buf, sizeof(buf),
				       timeout_s * 1000);
	(void) close(pipefd[0]);

	if (n < 0 && errno == ETIMEDOUT) {
		warning("lookup_software: timeout (%ds) cluster=%s app=%s",
		        timeout_s, cluster, app);
		(void) kill(pid, SIGKILL);
		(void) brokerd_waitpid_timeout(pid, 1);
		return BROKERD_ERR_LOOKUP_TIMEOUT;
	}
	if (n < 0) {
		error("lookup_software: read pipe: %m");
		(void) brokerd_waitpid_timeout(pid, 1);
		return BROKERD_ERR_LOOKUP_FAILED;
	}

	/* Reap the child; tight 1s window since stdout has already closed
	 * (child either exited or is just about to). */
	rc = brokerd_waitpid_timeout(pid, 1);
	if (rc != 0) {
		debug("lookup_software: cluster=%s app=%s exit=%d",
		      cluster, app, rc);
		return BROKERD_ERR_LOOKUP_FAILED;
	}

	/* Trim trailing newline / whitespace; consume only first line. */
	nl = strpbrk(buf, "\r\n");
	if (nl)
		*nl = '\0';
	for (p = buf + strlen(buf); p > buf; p--) {
		char c = p[-1];

		if (c != ' ' && c != '\t')
			break;
		p[-1] = '\0';
	}

	if (!buf[0]) {
		debug("lookup_software: cluster=%s app=%s returned empty stdout",
		      cluster, app);
		return BROKERD_ERR_LOOKUP_FAILED;
	}
	if (buf[0] != '/') {
		error("lookup_software: cluster=%s app=%s returned non-absolute path '%s'",
		      cluster, app, buf);
		return BROKERD_ERR_LOOKUP_FAILED;
	}

	if (out_path)
		*out_path = xstrdup(buf);
	debug("lookup_software: cluster=%s app=%s -> %s",
	      cluster, app, buf);
	return SLURM_SUCCESS;
}
