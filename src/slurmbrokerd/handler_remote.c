/*****************************************************************************\
 *  handler_remote.c - implementation of broker -> broker inbound handlers.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See handler_remote.h for the
 *  contract and doc/checklists/M07-handler-remote.md for the design.
 *
 *  M07 PR scope
 *  ------------
 *  All five RECEIVER-side RPCs are implemented:
 *
 *    BROKER_FORWARD_JOB    - hop check, idempotent insert, mkdir dst dir
 *    BROKER_STAGED_IN      - sudo sbatch the staged script, capture
 *                            remote_job_id, transition SUBMITTED/FAILED
 *    BROKER_QUERY_STATUS   - slurm_load_job per remote_job_id, build
 *                            response entries
 *    BROKER_CANCEL         - slurm_kill_job(remote_job_id, SIGTERM)
 *    BROKER_CLEANUP        - sudo rm -rf dst_work_dir, drop from table
 *
 *  Slurm-version independence
 *  --------------------------
 *  We deliberately invoke `sbatch` via fork+execvp rather than
 *  slurm_submit_batch_job() so that the wire payload from the
 *  originator broker can stay free of any job_desc_msg_t.
 *  slurm_kill_job() and slurm_load_job() are still used because they
 *  talk only to the LOCAL ctld, which is the same Slurm version as
 *  this broker process — no cross-version coupling.
 *
 *  Threading model
 *  ---------------
 *  Each handler runs synchronously on the listener thread (M05). They
 *  may fork+wait for short-lived child processes (sudo mkdir / sudo
 *  sbatch / sudo rm -rf), each bounded by an explicit timeout.
 *  Optimization to a worker pool is M07-T6 future work; MVP stays
 *  single-threaded per checklist §1.2.
\*****************************************************************************/

#include "config.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/pack.h"
#include "src/common/slurm_protocol_socket.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "broker_conf.h"
#include "broker_job.h"
#include "handler_remote.h"
#include "persist.h"
#include "proto.h"
#include "user_mapping.h"

#define DST_WORK_DIR_PREFIX     "/work/home/"  /* template root */
#define SUDO_BIN                "/usr/bin/sudo"
#define SBATCH_CHILD_TIMEOUT_S  60             /* sudo sbatch */
#define MKDIR_CHILD_TIMEOUT_S   10             /* sudo mkdir */
#define RM_CHILD_TIMEOUT_S      30             /* sudo rm -rf */

/*****************************************************************************\
 *                       reply helpers
\*****************************************************************************/

/* Send one private wire frame; payload ownership stays with caller. */
static int _reply(int conn_fd, uint16_t msg_type, void *payload)
{
	buf_t *buf = init_buf(BUF_SIZE);
	ssize_t sent;

	if (!buf) {
		error("%s: init_buf failed", __func__);
		return SLURM_ERROR;
	}
	if (brokerd_wire_build(buf, msg_type, payload,
			       SLURM_PROTOCOL_VERSION) != SLURM_SUCCESS) {
		FREE_NULL_BUFFER(buf);
		return SLURM_ERROR;
	}
	sent = slurm_msg_sendto(conn_fd, get_buf_data(buf),
				get_buf_offset(buf));
	FREE_NULL_BUFFER(buf);
	if (sent < 0) {
		error("%s: slurm_msg_sendto failed: %m", __func__);
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

static void _send_broker_ack(int conn_fd, int err, const char *trace_id,
			     const char *dst_work_dir)
{
	brokerd_broker_ack_msg_t resp = {
		.error_code   = (uint32_t) err,
		.trace_id     = (char *) (trace_id ? trace_id : ""),
		.dst_work_dir = (char *) (dst_work_dir ? dst_work_dir : ""),
	};
	(void) _reply(conn_fd, BROKERD_RESPONSE_BROKER_ACK, &resp);
}

static void _send_broker_submitted(int conn_fd, int err, const char *trace_id,
				   uint32_t remote_job_id)
{
	brokerd_broker_submitted_msg_t resp = {
		.error_code    = (uint32_t) err,
		.trace_id      = (char *) (trace_id ? trace_id : ""),
		.remote_job_id = remote_job_id,
	};
	(void) _reply(conn_fd, BROKERD_RESPONSE_BROKER_SUBMITTED, &resp);
}

/*****************************************************************************\
 *                       path helpers
\*****************************************************************************/

/* Portable basename: returns a pointer into `path` (not a fresh string). */
static const char *_path_basename(const char *path)
{
	const char *p;

	if (!path || !*path)
		return "";
	p = strrchr(path, '/');
	return p ? p + 1 : path;
}

/*
 * Whitelist check before passing a path to `rm -rf`. The receiver
 * dst_work_dir is always built from g_broker_conf and the remote_user
 * name, so a path that does not start with DST_WORK_DIR_PREFIX
 * indicates either a corrupted state file or a hostile RPC; we refuse.
 */
static bool _dst_work_dir_safe(const char *path)
{
	if (!path || !*path)
		return false;
	if (strncmp(path, DST_WORK_DIR_PREFIX,
		    sizeof(DST_WORK_DIR_PREFIX) - 1))
		return false;
	if (strstr(path, "/.."))
		return false;
	return true;
}

/*****************************************************************************\
 *                       fork+exec helpers
\*****************************************************************************/

/*
 * Wait for `pid` for at most `timeout_s` seconds; on timeout SIGKILL
 * and reap. Returns SLURM_SUCCESS only when the child exited normally
 * with status 0.
 */
static int _waitpid_timeout(pid_t pid, int timeout_s)
{
	int wstat = 0;
	int slept_us = 0;
	const int step_us = 100 * 1000; /* 100ms */
	const int budget_us = timeout_s * 1000 * 1000;

	while (slept_us < budget_us) {
		pid_t r = waitpid(pid, &wstat, WNOHANG);

		if (r == pid)
			break;
		if (r < 0 && errno != EINTR) {
			error("%s: waitpid: %m", __func__);
			return SLURM_ERROR;
		}
		usleep(step_us);
		slept_us += step_us;
	}

	if (slept_us >= budget_us) {
		warning("%s: child pid=%d exceeded %ds timeout, sending SIGKILL",
		        __func__, (int) pid, timeout_s);
		(void) kill(pid, SIGKILL);
		(void) waitpid(pid, &wstat, 0);
		return SLURM_ERROR;
	}

	if (!WIFEXITED(wstat)) {
		error("%s: child pid=%d killed by signal", __func__, (int) pid);
		return SLURM_ERROR;
	}
	if (WEXITSTATUS(wstat) != 0) {
		error("%s: child pid=%d exited with status=%d",
		      __func__, (int) pid, WEXITSTATUS(wstat));
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

/*
 * Run "sudo -u <remote_user> /bin/sh -c 'mkdir -p <dir> && chmod 700 <dir>'".
 * Both the directory creation and the mode bit set need the remote
 * user's uid so the resulting owner is correct without a chown.
 */
static int _create_dst_work_dir(const char *remote_user, const char *dir)
{
	pid_t pid;

	if (!remote_user || !*remote_user || !dir || !*dir)
		return SLURM_ERROR;
	if (!_dst_work_dir_safe(dir)) {
		error("%s: refusing unsafe dst_work_dir %s", __func__, dir);
		return SLURM_ERROR;
	}

	pid = fork();
	if (pid < 0) {
		error("%s: fork: %m", __func__);
		return SLURM_ERROR;
	}
	if (pid == 0) {
		/* child */
		execl(SUDO_BIN, "sudo", "-n", "-u", remote_user,
		      "/bin/sh", "-c",
		      "mkdir -p \"$1\" && chmod 700 \"$1\"",
		      "_brokerd_mkdir", dir, (char *) NULL);
		_exit(127);
	}
	return _waitpid_timeout(pid, MKDIR_CHILD_TIMEOUT_S);
}

/*
 * Run "sudo -u <remote_user> /bin/rm -rf <dir>" with timeout.
 */
static int _exec_sudo_rm_rf(const char *remote_user, const char *dir)
{
	pid_t pid;

	if (!remote_user || !*remote_user || !dir || !*dir)
		return SLURM_ERROR;
	if (!_dst_work_dir_safe(dir)) {
		error("%s: refusing unsafe rm -rf path %s", __func__, dir);
		return SLURM_ERROR;
	}

	pid = fork();
	if (pid < 0) {
		error("%s: fork: %m", __func__);
		return SLURM_ERROR;
	}
	if (pid == 0) {
		execl(SUDO_BIN, "sudo", "-n", "-u", remote_user,
		      "/bin/rm", "-rf", dir, (char *) NULL);
		_exit(127);
	}
	return _waitpid_timeout(pid, RM_CHILD_TIMEOUT_S);
}

/*
 * Parse the canonical sbatch stdout line:
 *     "Submitted batch job 12345"
 * into a uint32 job id. Returns 0 on parse failure.
 */
static uint32_t _parse_sbatch_jobid(const char *line)
{
	const char *needle = "Submitted batch job ";
	const char *p;
	unsigned long v;
	char *end = NULL;

	if (!line)
		return 0;
	p = strstr(line, needle);
	if (!p)
		return 0;
	p += strlen(needle);
	v = strtoul(p, &end, 10);
	if (end == p || v == 0 || v > UINT32_MAX)
		return 0;
	return (uint32_t) v;
}

/*
 * Run "sudo -u <remote_user> sbatch --partition=<part> --chdir=<cwd>
 * <cwd>/<basename(script_path)>" and capture stdout via pipe(2). On
 * success *out_jobid is the remote_job_id; on any failure returns
 * SLURM_ERROR with *out_jobid = 0.
 */
static int _sudo_sbatch(broker_job_t *job, uint32_t *out_jobid)
{
	int pipefd[2] = { -1, -1 };
	pid_t pid;
	char buf[512];
	ssize_t n;
	const char *script_basename;
	char *script_full = NULL;

	*out_jobid = 0;

	if (!job || !job->remote_user_name || !job->dst_work_dir ||
	    !job->target_partition || !job->script_path) {
		error("%s: missing required field on job %s",
		      __func__, job ? job->trace_id : "(null)");
		return SLURM_ERROR;
	}

	script_basename = _path_basename(job->script_path);
	if (!*script_basename) {
		error("%s: empty basename(script_path) for trace_id=%s",
		      __func__, job->trace_id);
		return SLURM_ERROR;
	}
	xstrfmtcat(script_full, "%s/%s",
		   job->dst_work_dir, script_basename);

	if (pipe(pipefd) < 0) {
		error("%s: pipe: %m", __func__);
		xfree(script_full);
		return SLURM_ERROR;
	}

	pid = fork();
	if (pid < 0) {
		error("%s: fork: %m", __func__);
		(void) close(pipefd[0]);
		(void) close(pipefd[1]);
		xfree(script_full);
		return SLURM_ERROR;
	}
	if (pid == 0) {
		char part_arg[512];
		char chdir_arg[1024];

		/* child: redirect stdout to pipe write end */
		(void) close(pipefd[0]);
		if (pipefd[1] != STDOUT_FILENO) {
			(void) dup2(pipefd[1], STDOUT_FILENO);
			(void) close(pipefd[1]);
		}
		/* keep stderr to the broker journal for diagnostics */
		(void) snprintf(part_arg, sizeof(part_arg),
				"--partition=%s", job->target_partition);
		(void) snprintf(chdir_arg, sizeof(chdir_arg),
				"--chdir=%s", job->dst_work_dir);
		execl(SUDO_BIN, "sudo", "-n", "-u", job->remote_user_name,
		      "sbatch", part_arg, chdir_arg, "--parsable",
		      script_full, (char *) NULL);
		_exit(127);
	}

	/* parent */
	(void) close(pipefd[1]);
	memset(buf, 0, sizeof(buf));
	{
		size_t off = 0;
		while (off + 1 < sizeof(buf)) {
			n = read(pipefd[0], buf + off, sizeof(buf) - 1 - off);
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
	}
	(void) close(pipefd[0]);

	if (_waitpid_timeout(pid, SBATCH_CHILD_TIMEOUT_S) != SLURM_SUCCESS) {
		xfree(script_full);
		return SLURM_ERROR;
	}

	/*
	 * --parsable prints just "<jobid>[;<cluster>]" to stdout; if the
	 * site disabled --parsable handling we fall back to the default
	 * "Submitted batch job <id>" message.
	 */
	{
		uint32_t v;
		char *end = NULL;
		unsigned long ul = strtoul(buf, &end, 10);

		if (end != buf && ul > 0 && ul <= UINT32_MAX) {
			v = (uint32_t) ul;
		} else {
			v = _parse_sbatch_jobid(buf);
		}
		if (!v) {
			error("%s: cannot parse sbatch stdout for trace_id=%s: '%s'",
			      __func__, job->trace_id, buf);
			xfree(script_full);
			return SLURM_ERROR;
		}
		*out_jobid = v;
	}

	xfree(script_full);
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       remote-state query helper
\*****************************************************************************/

/*
 * Populate `entry` with the remote ctld's view of `remote_job_id` via
 * slurm_load_job(). Caller owns entry->trace_id (already set); this
 * function fills the rest. Missing/unknown jobs are reported as
 * remote_state=JOB_PENDING which lets the originator's state machine
 * keep waiting without aborting.
 */
static void _query_remote_job_state(uint32_t remote_job_id,
				    brokerd_broker_status_entry_t *entry)
{
	job_info_msg_t *info_msg = NULL;
	slurm_job_info_t *ji;

	if (!remote_job_id) {
		entry->remote_state = JOB_PENDING;
		return;
	}
	if (slurm_load_job(&info_msg, remote_job_id, SHOW_DETAIL) ||
	    !info_msg || info_msg->record_count == 0) {
		entry->remote_state = JOB_PENDING;
		if (info_msg)
			slurm_free_job_info_msg(info_msg);
		return;
	}
	ji = &info_msg->job_array[0];
	entry->remote_state      = ji->job_state;
	entry->remote_start_time = ji->start_time;
	entry->remote_end_time   = ji->end_time;
	entry->remote_exit_code  = (int32_t) ji->exit_code;
	if (ji->tres_alloc_str)
		entry->remote_alloc_tres = xstrdup(ji->tres_alloc_str);
	slurm_free_job_info_msg(info_msg);
}

/*****************************************************************************\
 *                       handle_broker_forward_job (M07-T1)
\*****************************************************************************/

int handle_broker_forward_job(void *payload, int conn_fd)
{
	brokerd_broker_forward_job_msg_t *m = payload;
	broker_job_t *job;
	user_mapping_t *map;
	char *dst_work_dir = NULL;

	if (!m || !m->trace_id || !*m->trace_id) {
		error("handle_broker_forward_job: missing trace_id");
		_send_broker_ack(conn_fd, SLURM_ERROR, "", NULL);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_FORWARD_JOB,
				      payload);
		return SLURM_SUCCESS;
	}

	/* 1. hop limit (single-hop only in MVP). */
	if (m->hop_count > 0) {
		warning("handle_broker_forward_job: rejecting trace_id=%s hop_count=%u",
		        m->trace_id, m->hop_count);
		_send_broker_ack(conn_fd, BROKERD_ERR_HOP_EXCEEDED,
				 m->trace_id, NULL);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_FORWARD_JOB,
				      payload);
		return SLURM_SUCCESS;
	}

	/* 2. Idempotency: duplicate trace_id replays the original ACK. */
	job = broker_job_table_get(m->trace_id);
	if (job) {
		info("handle_broker_forward_job: trace_id=%s already RECEIVER, replaying ACK",
		     m->trace_id);
		_send_broker_ack(conn_fd, SLURM_SUCCESS, m->trace_id,
				 job->dst_work_dir);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_FORWARD_JOB,
				      payload);
		return SLURM_SUCCESS;
	}

	/* 3. user_mapping reverse-match: the originator's claim
	 * (src_user_name -> remote_user_name) MUST agree with the local
	 * user_mapping table. Otherwise an originator could ask us to
	 * impersonate any local user. */
	map = user_mapping_lookup(m->src_user_name, m->src_cluster);
	if (!map) {
		error("handle_broker_forward_job: no user_mapping for %s -> %s",
		      m->src_user_name ? m->src_user_name : "(null)",
		      m->src_cluster ? m->src_cluster : "(null)");
		_send_broker_ack(conn_fd, BROKERD_ERR_NO_USER_MAPPING,
				 m->trace_id, NULL);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_FORWARD_JOB,
				      payload);
		return SLURM_SUCCESS;
	}
	if (!m->remote_user_name ||
	    xstrcmp(map->remote_user, m->remote_user_name)) {
		warning("handle_broker_forward_job: user_mapping mismatch for trace_id=%s (claim=%s table=%s)",
		        m->trace_id,
		        m->remote_user_name ? m->remote_user_name : "(null)",
		        map->remote_user);
		_send_broker_ack(conn_fd, BROKERD_ERR_USER_MAPPING_MISMATCH,
				 m->trace_id, NULL);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_FORWARD_JOB,
				      payload);
		return SLURM_SUCCESS;
	}

	/* 4. Build dst_work_dir using the MVP template. */
	xstrfmtcat(dst_work_dir, "%s%s/.burst/%s/%u",
		   DST_WORK_DIR_PREFIX, map->remote_user,
		   m->src_cluster, m->src_job_id);

	if (_create_dst_work_dir(map->remote_user, dst_work_dir)
	    != SLURM_SUCCESS) {
		error("handle_broker_forward_job: failed to create %s",
		      dst_work_dir);
		_send_broker_ack(conn_fd, BROKERD_ERR_STAGE_FAILED,
				 m->trace_id, NULL);
		xfree(dst_work_dir);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_FORWARD_JOB,
				      payload);
		return SLURM_SUCCESS;
	}

	/* 5. Build broker_job_t (RECEIVER role, INIT state). */
	job = broker_job_create();
	(void) snprintf(job->trace_id, sizeof(job->trace_id), "%s",
			m->trace_id);
	job->src_job_id       = m->src_job_id;
	job->src_cluster      = xstrdup(m->src_cluster);
	job->src_user_name    = xstrdup(m->src_user_name);
	job->dst_cluster      = xstrdup(g_broker_conf.cluster_name);
	job->target_partition = xstrdup(m->target_partition);
	job->remote_user_name = xstrdup(map->remote_user);
	job->remote_uid       = map->remote_uid;
	job->remote_gid       = map->remote_gid;
	job->script_path      = xstrdup(m->script_path);
	job->app_name         = xstrdup(m->app_name);
	job->dst_work_dir     = dst_work_dir; /* ownership transfer */
	dst_work_dir = NULL;
	job->role             = BROKER_ROLE_RECEIVER;
	job->hop_count        = m->hop_count + 1;
	job->state            = BROKER_STATE_INIT;
	job->state_enter_time = time(NULL);
	job->submit_time      = job->state_enter_time;

	if (broker_job_table_add(job) != SLURM_SUCCESS) {
		/*
		 * Race: another listener path or M09 already registered the
		 * same trace_id. Treat as idempotent success and rely on
		 * the existing entry's dst_work_dir (already returned in
		 * the earlier ACK).
		 */
		info("handle_broker_forward_job: race on trace_id=%s, leaving existing entry",
		     m->trace_id);
		broker_job_destroy(job);
		_send_broker_ack(conn_fd, SLURM_SUCCESS, m->trace_id, NULL);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_FORWARD_JOB,
				      payload);
		return SLURM_SUCCESS;
	}
	persist_async_request();

	info("handle_broker_forward_job: trace_id=%s RECEIVER created, dst=%s app=%s script=%s",
	     job->trace_id, job->dst_work_dir,
	     job->app_name ? job->app_name : "(none)",
	     job->script_path ? _path_basename(job->script_path) : "(none)");

	_send_broker_ack(conn_fd, SLURM_SUCCESS, job->trace_id,
			 job->dst_work_dir);
	brokerd_free_msg_data(BROKERD_REQUEST_BROKER_FORWARD_JOB, payload);
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       handle_broker_staged_in (M07-T2)
\*****************************************************************************/

int handle_broker_staged_in(void *payload, int conn_fd)
{
	brokerd_broker_staged_in_msg_t *m = payload;
	broker_job_t *job;
	uint32_t remote_job_id = 0;

	if (!m || !m->trace_id || !*m->trace_id) {
		error("handle_broker_staged_in: missing trace_id");
		_send_broker_submitted(conn_fd, SLURM_ERROR, "", 0);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_STAGED_IN,
				      payload);
		return SLURM_SUCCESS;
	}

	job = broker_job_table_get(m->trace_id);
	if (!job || job->role != BROKER_ROLE_RECEIVER) {
		warning("handle_broker_staged_in: trace_id=%s not RECEIVER",
		        m->trace_id);
		_send_broker_submitted(conn_fd, BROKERD_ERR_NOT_FOUND,
				       m->trace_id, 0);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_STAGED_IN,
				      payload);
		return SLURM_SUCCESS;
	}

	/* Idempotency: already submitted, just replay the remote_job_id. */
	if (job->remote_job_id) {
		info("handle_broker_staged_in: trace_id=%s already submitted as remote_job_id=%u",
		     m->trace_id, job->remote_job_id);
		_send_broker_submitted(conn_fd, SLURM_SUCCESS,
				       job->trace_id, job->remote_job_id);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_STAGED_IN,
				      payload);
		return SLURM_SUCCESS;
	}

	/*
	 * TODO M12-T1: rewrite_job_script(job) - rewrite the staged
	 * script's `source ...` lines using lookup_software.sh
	 * <remote_cluster> <app_name> before invoking sbatch. M07
	 * intentionally submits the unmodified script so this PR can be
	 * verified end-to-end before M12 lands.
	 */

	if (_sudo_sbatch(job, &remote_job_id) != SLURM_SUCCESS ||
	    !remote_job_id) {
		error("handle_broker_staged_in: sbatch failed for trace_id=%s",
		      m->trace_id);
		broker_job_set_state(job, BROKER_STATE_FAILED,
				     "sbatch failed");
		persist_async_request();
		_send_broker_submitted(conn_fd,
				       BROKERD_ERR_REMOTE_SUBMIT_FAILED,
				       m->trace_id, 0);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_STAGED_IN,
				      payload);
		return SLURM_SUCCESS;
	}

	slurm_mutex_lock(&job->lock);
	job->remote_job_id  = remote_job_id;
	job->submit_time    = time(NULL);
	slurm_mutex_unlock(&job->lock);
	broker_job_set_state(job, BROKER_STATE_SUBMITTED, NULL);
	persist_async_request();

	info("handle_broker_staged_in: trace_id=%s -> remote_job_id=%u (state=SUBMITTED)",
	     job->trace_id, remote_job_id);

	_send_broker_submitted(conn_fd, SLURM_SUCCESS,
			       job->trace_id, remote_job_id);
	brokerd_free_msg_data(BROKERD_REQUEST_BROKER_STAGED_IN, payload);
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       handle_broker_query_status (M07-T3)
\*****************************************************************************/

int handle_broker_query_status(void *payload, int conn_fd)
{
	brokerd_broker_query_status_msg_t *m = payload;
	brokerd_broker_status_msg_t resp = { 0 };

	if (!m || m->trace_id_count == 0) {
		debug("handle_broker_query_status: empty query");
		(void) _reply(conn_fd, BROKERD_RESPONSE_BROKER_STATUS, &resp);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_QUERY_STATUS,
				      payload);
		return SLURM_SUCCESS;
	}

	resp.entry_count = m->trace_id_count;
	resp.entries     = xcalloc(m->trace_id_count, sizeof(*resp.entries));

	for (uint32_t i = 0; i < m->trace_id_count; i++) {
		brokerd_broker_status_entry_t *e = &resp.entries[i];
		broker_job_t *job;
		const char *tid = m->trace_ids ? m->trace_ids[i] : NULL;

		e->trace_id = xstrdup(tid ? tid : "");
		if (!tid || !*tid) {
			e->remote_state = JOB_PENDING;
			continue;
		}

		job = broker_job_table_get(tid);
		if (!job || job->role != BROKER_ROLE_RECEIVER) {
			e->remote_state = JOB_PENDING;
			continue;
		}
		_query_remote_job_state(job->remote_job_id, e);
	}

	(void) _reply(conn_fd, BROKERD_RESPONSE_BROKER_STATUS, &resp);

	/* free the entry strings + array we just built */
	for (uint32_t i = 0; i < resp.entry_count; i++) {
		xfree(resp.entries[i].trace_id);
		xfree(resp.entries[i].remote_alloc_tres);
	}
	xfree(resp.entries);

	debug("handle_broker_query_status: replied %u entries",
	      m->trace_id_count);

	brokerd_free_msg_data(BROKERD_REQUEST_BROKER_QUERY_STATUS, payload);
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       handle_broker_cancel (M07-T4)
\*****************************************************************************/

int handle_broker_cancel(void *payload, int conn_fd)
{
	brokerd_broker_cancel_msg_t *m = payload;
	broker_job_t *job;

	if (!m || !m->trace_id || !*m->trace_id) {
		error("handle_broker_cancel: missing trace_id");
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_CANCEL, payload);
		return SLURM_SUCCESS;
	}

	job = broker_job_table_get(m->trace_id);
	if (!job || job->role != BROKER_ROLE_RECEIVER) {
		debug("handle_broker_cancel: trace_id=%s not RECEIVER (already reaped?)",
		      m->trace_id);
		/*
		 * Idempotent contract: originator may resend cancel after
		 * we've already torn down. No reply is wired for cancel
		 * (fire-and-forget per design doc §6).
		 */
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_CANCEL, payload);
		return SLURM_SUCCESS;
	}

	if (job->remote_job_id) {
		int rc = slurm_kill_job(job->remote_job_id, SIGTERM,
					KILL_FULL_JOB);
		if (rc) {
			/* Best-effort: the job may have already finished;
			 * we still mark broker state CANCELLED so the
			 * originator stops polling. */
			warning("handle_broker_cancel: slurm_kill_job(%u): %s",
			        job->remote_job_id, slurm_strerror(rc));
		}
	}

	slurm_mutex_lock(&job->lock);
	job->cancel_propagated = true;
	slurm_mutex_unlock(&job->lock);
	broker_job_set_state(job, BROKER_STATE_CANCELLED,
			     "cancel by originator");
	persist_async_request();

	info("handle_broker_cancel: trace_id=%s remote_job_id=%u cancelled",
	     job->trace_id, job->remote_job_id);

	(void) conn_fd; /* fire-and-forget: no response frame */
	brokerd_free_msg_data(BROKERD_REQUEST_BROKER_CANCEL, payload);
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       handle_broker_cleanup (M07-T5)
\*****************************************************************************/

int handle_broker_cleanup(void *payload, int conn_fd)
{
	brokerd_broker_cleanup_msg_t *m = payload;
	broker_job_t *job;

	(void) conn_fd; /* fire-and-forget */

	if (!m || !m->trace_id || !*m->trace_id) {
		error("handle_broker_cleanup: missing trace_id");
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_CLEANUP, payload);
		return SLURM_SUCCESS;
	}

	job = broker_job_table_get(m->trace_id);
	if (!job) {
		debug("handle_broker_cleanup: trace_id=%s not in table (idempotent)",
		      m->trace_id);
		brokerd_free_msg_data(BROKERD_REQUEST_BROKER_CLEANUP, payload);
		return SLURM_SUCCESS;
	}

	if (job->dst_work_dir && job->remote_user_name) {
		if (_exec_sudo_rm_rf(job->remote_user_name,
				     job->dst_work_dir) != SLURM_SUCCESS) {
			/* Don't block table cleanup on a stale dst dir; the
			 * MaxInFlight slot must be reclaimed regardless. The
			 * dir will be picked up by the periodic reaper later. */
			warning("handle_broker_cleanup: rm -rf %s failed; dropping table entry anyway",
			        job->dst_work_dir);
		}
	}

	(void) broker_job_table_remove(m->trace_id);
	persist_async_request();

	info("handle_broker_cleanup: trace_id=%s reaped", m->trace_id);
	brokerd_free_msg_data(BROKERD_REQUEST_BROKER_CLEANUP, payload);
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       dispatcher
\*****************************************************************************/

void dispatch_remote_msg(uint16_t msg_type, void *payload,
			 int conn_fd, slurm_addr_t *peer_addr)
{
	char addr_str[INET6_ADDRSTRLEN] = "";

	if (peer_addr)
		slurm_get_ip_str(peer_addr, addr_str, sizeof(addr_str));
	debug2("dispatch_remote_msg: msg_type=%s from %s",
	       brokerd_msg_type_str(msg_type), addr_str);

	switch (msg_type) {
	case BROKERD_REQUEST_BROKER_FORWARD_JOB:
		(void) handle_broker_forward_job(payload, conn_fd);
		break;
	case BROKERD_REQUEST_BROKER_STAGED_IN:
		(void) handle_broker_staged_in(payload, conn_fd);
		break;
	case BROKERD_REQUEST_BROKER_QUERY_STATUS:
		(void) handle_broker_query_status(payload, conn_fd);
		break;
	case BROKERD_REQUEST_BROKER_CANCEL:
		(void) handle_broker_cancel(payload, conn_fd);
		break;
	case BROKERD_REQUEST_BROKER_CLEANUP:
		(void) handle_broker_cleanup(payload, conn_fd);
		break;
	default:
		error("dispatch_remote_msg: unsupported msg_type=%s from %s",
		      brokerd_msg_type_str(msg_type), addr_str);
		brokerd_free_msg_data(msg_type, payload);
		break;
	}
}
