/*****************************************************************************\
 *  stage.c - asynchronous data-staging worker pool (rsync over SSH).
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See stage.h for the
 *  contract and doc/checklists/M10-stage.md for the design.
 *
 *  Worker thread layout
 *  --------------------
 *  Workers loop:
 *
 *    lock(stage_mutex)
 *    while (running && both queues empty)
 *        cond_wait(stage_cond, stage_mutex)
 *    if (!running) goto exit
 *    task = list_pop(stage_in_queue) or list_pop(stage_out_queue)
 *    unlock(stage_mutex)
 *
 *    fork()
 *      child : redirect stdout/stderr to log file
 *              execvp("/usr/bin/sudo", ["sudo", "-n", "-u", src_user,
 *                                       rsync_bin, "-av", ...])
 *      parent: waitpid_with_timeout(STAGE_CHILD_TIMEOUT_S)
 *
 *    if exit code == 0
 *        state_machine_transition(STAGED_IN | COMPLETED)
 *        if STAGE_IN: egress_staged_in_async()
 *    else
 *        log; let M09 watchdog drive the retry on next tick
 *
 *  Slurm-version independence
 *  --------------------------
 *  No libslurm RPC; only POSIX fork/execvp/waitpid + Slurm-internal
 *  helpers (xmalloc/xstrfmtcat/log) which are version-stable.
\*****************************************************************************/

#include "config.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/list.h"
#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "broker_conf.h"
#include "broker_job.h"
#include "egress.h"
#include "persist.h"
#include "stage.h"
#include "state_machine.h"

/*****************************************************************************\
 *                       module-local state
\*****************************************************************************/

#define STAGE_LOG_DIR              "/var/log/slurm/broker_stage"
#define STAGE_DEFAULT_WORKERS      4
#define STAGE_CHILD_TIMEOUT_S      (60 * 60)  /* 1h safety bound */
#define SUDO_BIN                   "/usr/bin/sudo"
#define DU_BIN                     "/usr/bin/du"

typedef enum {
	STAGE_IN  = 0,
	STAGE_OUT = 1,
} stage_dir_t;

typedef struct {
	char         trace_id[BROKER_TRACE_ID_LEN];
	stage_dir_t  direction;
} stage_task_t;

static list_t          *stage_in_queue;
static list_t          *stage_out_queue;
static pthread_mutex_t  stage_mutex;
static pthread_cond_t   stage_cond  = PTHREAD_COND_INITIALIZER;
static pthread_t       *stage_workers;
static int              n_workers;
static volatile bool    stage_running       = false;
static bool             stage_pool_active   = false;
static bool             stage_mutex_inited  = false;

/*****************************************************************************\
 *                       fork+exec helpers
\*****************************************************************************/

/*
 * waitpid with a wall-clock timeout. On timeout SIGKILL the child and
 * reap. Returns the child's exit status (>= 0) on normal exit, -1 on
 * any failure / timeout / signal kill.
 */
static int _waitpid_timeout(pid_t pid, int timeout_s)
{
	int wstat = 0;
	int slept_us = 0;
	const int step_us = 100 * 1000;
	const int budget_us = timeout_s * 1000 * 1000;

	while (slept_us < budget_us) {
		pid_t r = waitpid(pid, &wstat, WNOHANG);

		if (r == pid)
			break;
		if (r < 0 && errno != EINTR) {
			error("%s: waitpid: %m", __func__);
			return -1;
		}
		usleep(step_us);
		slept_us += step_us;
	}

	if (slept_us >= budget_us) {
		warning("%s: child pid=%d exceeded %ds timeout, sending SIGKILL",
		        __func__, (int) pid, timeout_s);
		(void) kill(pid, SIGKILL);
		(void) waitpid(pid, &wstat, 0);
		return -1;
	}

	if (!WIFEXITED(wstat))
		return -1;
	return WEXITSTATUS(wstat);
}

/*****************************************************************************\
 *                       du -sb estimator (M10-T4)
\*****************************************************************************/

/*
 * Run `sudo -u <user> du -sb <path>` and parse the leading uint64.
 * Returns UINT64_MAX on any failure (caller treats this as
 * "indeterminate, refuse to proceed").
 */
static uint64_t _du_sb(const char *user, const char *path)
{
	int pipefd[2] = { -1, -1 };
	pid_t pid;
	char buf[128];
	uint64_t v;
	int rc;
	size_t off = 0;

	if (!user || !*user || !path || !*path)
		return UINT64_MAX;

	if (pipe(pipefd) < 0) {
		error("%s: pipe: %m", __func__);
		return UINT64_MAX;
	}

	pid = fork();
	if (pid < 0) {
		(void) close(pipefd[0]);
		(void) close(pipefd[1]);
		error("%s: fork: %m", __func__);
		return UINT64_MAX;
	}
	if (pid == 0) {
		(void) close(pipefd[0]);
		if (pipefd[1] != STDOUT_FILENO) {
			(void) dup2(pipefd[1], STDOUT_FILENO);
			(void) close(pipefd[1]);
		}
		execl(SUDO_BIN, "sudo", "-n", "-u", user, DU_BIN,
		      "-sb", path, (char *) NULL);
		_exit(127);
	}

	(void) close(pipefd[1]);
	memset(buf, 0, sizeof(buf));
	while (off + 1 < sizeof(buf)) {
		ssize_t n = read(pipefd[0], buf + off, sizeof(buf) - 1 - off);

		if (n > 0)
			off += (size_t) n;
		else if (n == 0)
			break;
		else if (errno == EINTR)
			continue;
		else
			break;
	}
	buf[off] = '\0';
	(void) close(pipefd[0]);

	rc = _waitpid_timeout(pid, 60);
	if (rc != 0) {
		debug("%s: du failed for user=%s path=%s rc=%d",
		      __func__, user, path, rc);
		return UINT64_MAX;
	}
	if (sscanf(buf, "%" SCNu64, &v) != 1) {
		debug("%s: cannot parse du output for path=%s: '%s'",
		      __func__, path, buf);
		return UINT64_MAX;
	}
	return v;
}

/*****************************************************************************\
 *                       rsync exec
\*****************************************************************************/

/*
 * Build a heap-allocated argv[] for "sudo -n -u <src_user>
 * <rsync_bin> -av [--delete] -e <ssh_string> <src> <dst>".
 *
 * argv ownership: the caller must xfree each non-NULL element and the
 * argv array itself; we use _free_argv() for this.
 */
static void _free_argv(char **argv)
{
	if (!argv)
		return;
	for (int i = 0; argv[i]; i++)
		xfree(argv[i]);
	xfree(argv);
}

static char **_build_rsync_argv(broker_job_t *job, stage_dir_t dir)
{
	char *ssh_e   = NULL;
	char *src     = NULL;
	char *dst     = NULL;
	const char *rsync_bin = g_broker_conf.stage_rsync_bin
				? g_broker_conf.stage_rsync_bin
				: "/usr/bin/rsync";
	char **argv;
	int idx = 0;

	if (!job->src_user_name || !*job->src_user_name)
		return NULL;
	if (!job->src_work_dir || !*job->src_work_dir)
		return NULL;
	if (!job->dst_work_dir || !*job->dst_work_dir)
		return NULL;
	if (!g_broker_conf.stage_ssh_key || !*g_broker_conf.stage_ssh_key)
		return NULL;
	if (!g_broker_conf.stage_ssh_user || !*g_broker_conf.stage_ssh_user)
		return NULL;
	if (!g_broker_conf.remote_broker_host ||
	    !*g_broker_conf.remote_broker_host)
		return NULL;

	xstrfmtcat(ssh_e,
		   "ssh -i %s -o StrictHostKeyChecking=no "
		   "-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
		   g_broker_conf.stage_ssh_key);

	if (dir == STAGE_IN) {
		xstrfmtcat(src, "%s/", job->src_work_dir);
		xstrfmtcat(dst, "%s@%s:%s/",
			   g_broker_conf.stage_ssh_user,
			   g_broker_conf.remote_broker_host,
			   job->dst_work_dir);
	} else {
		xstrfmtcat(src, "%s@%s:%s/",
			   g_broker_conf.stage_ssh_user,
			   g_broker_conf.remote_broker_host,
			   job->dst_work_dir);
		xstrfmtcat(dst, "%s/", job->src_work_dir);
	}

	/* Layout (max 13 slots so we never overflow):
	 *   [0]  sudo
	 *   [1]  -n
	 *   [2]  -u
	 *   [3]  <src_user>
	 *   [4]  <rsync_bin>
	 *   [5]  -av
	 *   [6]  --partial   (keep half-transferred files so a broker restart
	 *                     can resume rather than re-transfer from byte 0;
	 *                     small price: the receiver's directory may briefly
	 *                     contain partial files, but the next rsync run
	 *                     either completes or M14 cleans them up)
	 *   [7]  --delete    (STAGE_IN only; STAGE_OUT keeps remote files)
	 *   [8]  -e
	 *   [9]  <ssh_e>
	 *   [10] <src>
	 *   [11] <dst>
	 *   [12] NULL
	 */
	argv = xcalloc(13, sizeof(char *));
	argv[idx++] = xstrdup("sudo");
	argv[idx++] = xstrdup("-n");
	argv[idx++] = xstrdup("-u");
	argv[idx++] = xstrdup(job->src_user_name);
	argv[idx++] = xstrdup(rsync_bin);
	argv[idx++] = xstrdup("-av");
	argv[idx++] = xstrdup("--partial");
	if (dir == STAGE_IN)
		argv[idx++] = xstrdup("--delete");
	argv[idx++] = xstrdup("-e");
	argv[idx++] = ssh_e; ssh_e = NULL;
	argv[idx++] = src;   src   = NULL;
	argv[idx++] = dst;   dst   = NULL;
	argv[idx]   = NULL;

	return argv;
}

/*
 * Open (creating if needed) /var/log/slurm/broker_stage/<trace_id>.log
 * and dup2 it onto stdout & stderr in the calling process. Intended
 * to run inside the child between fork() and execvp(); on failure we
 * simply leave stdio attached to whatever the parent had, the exec
 * still proceeds.
 */
static void _redirect_child_log(const char *trace_id)
{
	char *path = NULL;
	int fd;

	(void) mkdir(STAGE_LOG_DIR, 0755);

	xstrfmtcat(path, "%s/%s.log", STAGE_LOG_DIR,
		   trace_id ? trace_id : "unknown");

	fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
	xfree(path);
	if (fd < 0)
		return; /* best-effort */
	(void) dup2(fd, STDOUT_FILENO);
	(void) dup2(fd, STDERR_FILENO);
	if (fd > STDERR_FILENO)
		(void) close(fd);
}

/* Returns child exit code (0 on success) or -1 on infrastructure failure. */
static int _exec_rsync(broker_job_t *job, stage_dir_t dir)
{
	char **argv;
	pid_t pid;
	int rc;

	argv = _build_rsync_argv(job, dir);
	if (!argv) {
		error("stage: missing fields for trace_id=%s, cannot build rsync argv",
		      job->trace_id);
		return -1;
	}

	pid = fork();
	if (pid < 0) {
		error("stage: fork: %m");
		_free_argv(argv);
		return -1;
	}
	if (pid == 0) {
		_redirect_child_log(job->trace_id);
		execvp(argv[0], argv);
		/* execvp only returns on failure */
		_exit(127);
	}

	_free_argv(argv);
	rc = _waitpid_timeout(pid, STAGE_CHILD_TIMEOUT_S);
	return rc;
}

/*****************************************************************************\
 *                       per-task driver
\*****************************************************************************/

static void _run_stage_task(int wid, stage_task_t *t)
{
	broker_job_t *job;
	int rc;

	job = broker_job_table_get(t->trace_id);
	if (!job) {
		debug("stage[w%d]: trace_id=%s no longer in table; skip",
		      wid, t->trace_id);
		return;
	}

	if (t->direction == STAGE_IN) {
		info("stage[w%d]: stage-in start trace_id=%s %s -> %s:%s",
		     wid, job->trace_id,
		     job->src_work_dir,
		     g_broker_conf.remote_broker_host,
		     job->dst_work_dir);
		rc = _exec_rsync(job, STAGE_IN);
		if (rc == 0) {
			info("stage[w%d]: stage-in done trace_id=%s",
			     wid, job->trace_id);
			state_machine_transition(job,
						 BROKER_STATE_STAGED_IN, NULL);
			(void) egress_staged_in_async(job);
		} else {
			error("stage[w%d]: stage-in FAILED trace_id=%s rc=%d (M09 watchdog will retry)",
			      wid, job->trace_id, rc);
			/* Do NOT transition FAILED here; M09 _on_staging_in
			 * already handles timeout-and-retry. Touching state
			 * from this thread would race with the state
			 * machine tick. */
		}
	} else {
		info("stage[w%d]: stage-out start trace_id=%s %s:%s -> %s",
		     wid, job->trace_id,
		     g_broker_conf.remote_broker_host,
		     job->dst_work_dir, job->src_work_dir);
		rc = _exec_rsync(job, STAGE_OUT);
		if (rc == 0) {
			info("stage[w%d]: stage-out done trace_id=%s",
			     wid, job->trace_id);
			state_machine_transition(job,
						 BROKER_STATE_COMPLETED,
						 "stage_out ok");
		} else {
			error("stage[w%d]: stage-out FAILED trace_id=%s rc=%d (M09 watchdog will retry)",
			      wid, job->trace_id, rc);
		}
	}
	persist_async_request();
}

/*****************************************************************************\
 *                       worker thread
\*****************************************************************************/

static void *_stage_worker_main(void *arg)
{
	int wid = (int) (intptr_t) arg;

	while (1) {
		stage_task_t *t = NULL;

		slurm_mutex_lock(&stage_mutex);
		while (stage_running &&
		       list_is_empty(stage_in_queue) &&
		       list_is_empty(stage_out_queue))
			slurm_cond_wait(&stage_cond, &stage_mutex);

		if (!stage_running) {
			slurm_mutex_unlock(&stage_mutex);
			break;
		}

		/* stage_in_queue priority: getting data to the receiver
		 * earlier reduces user-visible time-to-RUNNING. */
		t = list_pop(stage_in_queue);
		if (!t)
			t = list_pop(stage_out_queue);
		slurm_mutex_unlock(&stage_mutex);

		if (t) {
			_run_stage_task(wid, t);
			xfree(t);
		}
	}
	debug("stage[w%d]: exit", wid);
	return NULL;
}

/*****************************************************************************\
 *                       submission API
\*****************************************************************************/

static int _enqueue(stage_dir_t dir, broker_job_t *job)
{
	stage_task_t *t;

	if (!stage_pool_active) {
		error("stage_submit: pool not started, dropping trace_id=%s dir=%d",
		      job ? job->trace_id : "(null)", (int) dir);
		return SLURM_ERROR;
	}
	if (!job || !job->trace_id[0])
		return SLURM_ERROR;

	t = xmalloc(sizeof(*t));
	(void) snprintf(t->trace_id, sizeof(t->trace_id), "%s",
			job->trace_id);
	t->direction = dir;

	slurm_mutex_lock(&stage_mutex);
	if (dir == STAGE_IN)
		list_append(stage_in_queue, t);
	else
		list_append(stage_out_queue, t);
	slurm_cond_signal(&stage_cond);
	slurm_mutex_unlock(&stage_mutex);

	return SLURM_SUCCESS;
}

int stage_submit_in(broker_job_t *job)
{
	uint64_t bytes;

	if (!job)
		return SLURM_ERROR;

	if (g_broker_conf.max_stage_bytes && job->src_user_name &&
	    job->src_work_dir) {
		bytes = _du_sb(job->src_user_name, job->src_work_dir);
		if (bytes != UINT64_MAX &&
		    bytes > g_broker_conf.max_stage_bytes) {
			char reason[128];

			(void) snprintf(reason, sizeof(reason),
					"stage size %" PRIu64
					" exceeds max %" PRIu64,
					bytes,
					g_broker_conf.max_stage_bytes);
			error("stage_submit_in: trace_id=%s %s",
			      job->trace_id, reason);
			state_machine_transition(job,
						 BROKER_STATE_FAILED,
						 reason);
			return SLURM_ERROR;
		}
	}

	return _enqueue(STAGE_IN, job);
}

int stage_submit_out(broker_job_t *job)
{
	return _enqueue(STAGE_OUT, job);
}

/*****************************************************************************\
 *                       lifecycle
\*****************************************************************************/

static void _task_free(void *p)
{
	xfree(p);
}

int stage_pool_start(void)
{
	if (stage_pool_active) {
		debug("%s: already started", __func__);
		return SLURM_SUCCESS;
	}

	(void) mkdir(STAGE_LOG_DIR, 0755);

	stage_in_queue  = list_create(_task_free);
	stage_out_queue = list_create(_task_free);
	if (!stage_in_queue || !stage_out_queue) {
		error("stage_pool_start: list_create failed");
		FREE_NULL_LIST(stage_in_queue);
		FREE_NULL_LIST(stage_out_queue);
		return SLURM_ERROR;
	}

	slurm_mutex_init(&stage_mutex);
	stage_mutex_inited = true;
	slurm_cond_init(&stage_cond, NULL);

	n_workers = (int) g_broker_conf.stage_worker_count;
	if (n_workers <= 0)
		n_workers = STAGE_DEFAULT_WORKERS;

	stage_workers = xcalloc(n_workers, sizeof(pthread_t));

	stage_running     = true;
	stage_pool_active = true;

	for (int i = 0; i < n_workers; i++) {
		slurm_thread_create(&stage_workers[i],
				    _stage_worker_main,
				    (void *) (intptr_t) i);
	}

	info("stage: pool started, %d worker(s), log_dir=%s",
	     n_workers, STAGE_LOG_DIR);
	return SLURM_SUCCESS;
}

void stage_pool_stop(void)
{
	if (!stage_pool_active)
		return;

	slurm_mutex_lock(&stage_mutex);
	stage_running = false;
	pthread_cond_broadcast(&stage_cond);
	slurm_mutex_unlock(&stage_mutex);

	for (int i = 0; i < n_workers; i++)
		(void) pthread_join(stage_workers[i], NULL);
	xfree(stage_workers);
	stage_workers = NULL;
	n_workers = 0;

	FREE_NULL_LIST(stage_in_queue);
	FREE_NULL_LIST(stage_out_queue);

	slurm_cond_destroy(&stage_cond);
	if (stage_mutex_inited) {
		slurm_mutex_destroy(&stage_mutex);
		stage_mutex_inited = false;
	}

	stage_pool_active = false;
	info("stage: pool stopped");
}
