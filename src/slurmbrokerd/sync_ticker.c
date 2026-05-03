/*****************************************************************************\
 *  sync_ticker.c - ORIGINATOR-side periodic remote-state poller.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See sync_ticker.h for
 *  the contract and doc/checklists/M13-sync-ticker.md for the design.
 *
 *  Tick layout (3-phase, same model as state_machine.c)
 *  ----------------------------------------------------
 *    Phase 1: foreach (g_broker_jobs_lock held) -> _collect_trace_id
 *    Phase 2: egress_query_status_sync (lock released, blocking RPC)
 *    Phase 3: _apply_remote_status per-entry (job->lock per-write,
 *             then state_machine_transition / ctld_update_remote_state
 *             with no lock held during the network call).
\*****************************************************************************/

#include "config.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/select.h>
#include <sys/types.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/fd.h"
#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "broker_conf.h"
#include "broker_job.h"
#include "egress.h"
#include "proto.h"
#include "stage.h"
#include "state_machine.h"
#include "sync_ticker.h"

/*****************************************************************************\
 *                       module-local state
\*****************************************************************************/

static pthread_t sync_tid;
static volatile bool sync_running       = false;
static bool          sync_thread_active = false;
static uint32_t      consecutive_failures = 0;

/* Self-pipe wakeup (same trick as listener.c / state_machine.c) so
 * stop() can return within ~1ms instead of waiting up to one full
 * poll_interval. */
static int sync_wakeup_pipe[2] = { -1, -1 };
#define SYNC_WAKE_RD  sync_wakeup_pipe[0]
#define SYNC_WAKE_WR  sync_wakeup_pipe[1]

#define SYNC_DEFAULT_POLL_INTERVAL  10  /* seconds, matches design §6 */
#define SYNC_DEFAULT_POLL_RETRIES    5

/*****************************************************************************\
 *                       trace_id collection (Phase 1)
\*****************************************************************************/

typedef struct {
	char    **trace_ids;
	uint32_t  count;
	uint32_t  cap;
} _collect_t;

static int _collect_trace_id(broker_job_t *j, void *arg)
{
	_collect_t *c = arg;

	if (j->role != BROKER_ROLE_ORIGINATOR)
		return 0;
	if (j->state != BROKER_STATE_SUBMITTED &&
	    j->state != BROKER_STATE_RUNNING)
		return 0;

	/* Grow on demand: 16 -> 32 -> 64 ... */
	if (c->count == c->cap) {
		c->cap = c->cap ? c->cap * 2 : 16;
		xrealloc(c->trace_ids, c->cap * sizeof(char *));
	}
	c->trace_ids[c->count++] = xstrdup(j->trace_id);
	return 0;
}

static void _collect_free(_collect_t *c)
{
	if (!c->trace_ids)
		return;
	for (uint32_t i = 0; i < c->count; i++)
		xfree(c->trace_ids[i]);
	xfree(c->trace_ids);
	c->trace_ids = NULL;
	c->count = 0;
	c->cap = 0;
}

/*****************************************************************************\
 *                       _apply_remote_status (Phase 3, per entry)
 *
 * Locking model:
 *   - broker_job_table_get is the only call that takes
 *     g_broker_jobs_lock (briefly). It returns a stable pointer (the
 *     entry cannot be removed concurrently because state_machine /
 *     listener handlers are independent threads).
 *   - We hold job->lock while reading/writing per-job fields, then
 *     release before calling state_machine_transition (which takes
 *     job->lock itself - nested locking would deadlock) or
 *     ctld_update_remote_state (network RPC, must NOT hold any lock).
\*****************************************************************************/

static void _apply_remote_status(brokerd_broker_status_entry_t *e)
{
	broker_job_t *job;
	bool fields_changed = false;
	bool need_ctld_push = false;
	uint32_t saw_state;

	if (!e || !e->trace_id || !*e->trace_id)
		return;

	job = broker_job_table_get(e->trace_id);
	if (!job) {
		debug2("sync_ticker: trace_id=%s no longer in table, skip",
		       e->trace_id);
		return;
	}
	if (job->role != BROKER_ROLE_ORIGINATOR) {
		/* Receiver-side jobs do not poll themselves. */
		return;
	}

	/* --- short-locked field merge --- */
	slurm_mutex_lock(&job->lock);

	if (e->remote_alloc_tres &&
	    (!job->remote_alloc_tres ||
	     xstrcmp(job->remote_alloc_tres, e->remote_alloc_tres))) {
		xfree(job->remote_alloc_tres);
		job->remote_alloc_tres = xstrdup(e->remote_alloc_tres);
		fields_changed = true;
	}
	if (e->remote_start_time &&
	    job->remote_start_time != e->remote_start_time) {
		job->remote_start_time = e->remote_start_time;
		fields_changed = true;
	}
	if (e->remote_end_time &&
	    job->remote_end_time != e->remote_end_time) {
		job->remote_end_time = e->remote_end_time;
		fields_changed = true;
	}
	if (e->remote_exit_code != job->remote_exit_code) {
		job->remote_exit_code = e->remote_exit_code;
		fields_changed = true;
	}
	job->last_poll_time = time(NULL);

	saw_state = (uint32_t) job->state;
	slurm_mutex_unlock(&job->lock);

	/*
	 * State mapping (M13 §2.4):
	 *   JOB_PENDING                    -> push ctld update if changed
	 *   JOB_RUNNING                    -> SUBMITTED -> RUNNING + push
	 *   JOB_COMPLETE/FAILED/TIMEOUT    -> RUNNING/SUBMITTED -> STAGING_OUT
	 *   JOB_CANCELLED                  -> -> CANCELLED
	 *
	 * For PENDING / RUNNING we only push to ctld when something
	 * actually changed (either remote_state or one of the bookkeeping
	 * fields above) to avoid spamming the local ctld every 10s.
	 */
	switch (e->remote_state) {
	case JOB_PENDING:
		if (fields_changed)
			need_ctld_push = true;
		break;

	case JOB_RUNNING:
		if (saw_state == BROKER_STATE_SUBMITTED) {
			state_machine_transition(job, BROKER_STATE_RUNNING,
						 NULL);
			need_ctld_push = true;
		} else if (fields_changed) {
			need_ctld_push = true;
		}
		break;

	case JOB_COMPLETE:
	case JOB_FAILED:
	case JOB_TIMEOUT:
	case JOB_NODE_FAIL:
	case JOB_PREEMPTED:
	case JOB_BOOT_FAIL:
	case JOB_DEADLINE:
	case JOB_OOM:
		if (saw_state == BROKER_STATE_RUNNING ||
		    saw_state == BROKER_STATE_SUBMITTED) {
			/*
			 * Move to STAGING_OUT FIRST so any concurrent state
			 * machine tick observes the new state before the
			 * stage worker runs (avoids racing on the
			 * "transition COMPLETED on stage-out success" step).
			 * Then enqueue the reverse-direction rsync.
			 *
			 * No UPDATE push here: the terminal push happens
			 * inside state_machine _on_terminal once stage-out
			 * completes and transitions us to COMPLETED.
			 */
			state_machine_transition(job,
						 BROKER_STATE_STAGING_OUT,
						 NULL);
			(void) stage_submit_out(job);
		}
		break;

	case JOB_CANCELLED:
		if (saw_state != BROKER_STATE_CANCELLED &&
		    saw_state != BROKER_STATE_COMPLETED &&
		    saw_state != BROKER_STATE_FAILED) {
			state_machine_transition(job, BROKER_STATE_CANCELLED,
						 "remote cancelled");
		}
		break;

	default:
		debug("sync_ticker: trace_id=%s unhandled remote_state=%u",
		      job->trace_id, e->remote_state);
		break;
	}

	if (need_ctld_push) {
		/* Best-effort: failure means the next sync_ticker tick
		 * will retry naturally. ctld_update_remote_state itself
		 * already warning()s on failure, so no extra logging here. */
		(void) ctld_update_remote_state(job);
	}
}

/*****************************************************************************\
 *                       _sync_ticker_run (one tick body)
\*****************************************************************************/

static void _sync_ticker_run(void)
{
	_collect_t c = { 0 };
	brokerd_broker_status_msg_t *resp = NULL;
	int rc;

	/* Phase 1: under g_broker_jobs_lock, snapshot trace_ids. */
	broker_job_table_foreach(_collect_trace_id, &c);
	if (c.count == 0) {
		debug3("sync_ticker: no in-flight ORIGINATOR jobs");
		_collect_free(&c);
		return;
	}

	debug2("sync_ticker: querying %u trace_ids", c.count);

	/* Phase 2: blocking RPC, no locks held. */
	rc = egress_query_status_sync(c.trace_ids, c.count, &resp);
	if (rc != SLURM_SUCCESS || !resp) {
		consecutive_failures++;
		if (consecutive_failures <
		    g_broker_conf.poll_max_retries) {
			warning("sync_ticker: query_status #%u failed: %s",
			        consecutive_failures,
			        slurm_strerror(rc));
		} else {
			error("sync_ticker: peer unreachable for %u rounds (%us each); leaving SUBMITTED jobs to M09 watchdog",
			      consecutive_failures,
			      g_broker_conf.poll_interval ?
			      g_broker_conf.poll_interval :
			      SYNC_DEFAULT_POLL_INTERVAL);
		}
		_collect_free(&c);
		if (resp)
			brokerd_free_broker_status_msg(resp);
		return;
	}
	consecutive_failures = 0;

	/* Phase 3: apply per entry. */
	for (uint32_t i = 0; i < resp->entry_count; i++)
		_apply_remote_status(&resp->entries[i]);

	debug("sync_ticker: applied %u entries", resp->entry_count);

	_collect_free(&c);
	brokerd_free_broker_status_msg(resp);
}

/*****************************************************************************\
 *                       tick driver
\*****************************************************************************/

static void _drain_wakeup_pipe(void)
{
	char tmp[64];
	ssize_t n;

	do {
		n = read(SYNC_WAKE_RD, tmp, sizeof(tmp));
	} while (n > 0 || (n < 0 && errno == EINTR));
}

static void *_sync_ticker_main(void *arg)
{
	(void) arg;

	while (sync_running) {
		fd_set rfds;
		struct timeval tv;
		uint32_t poll_secs;
		int n;

		_sync_ticker_run();
		if (!sync_running)
			break;

		poll_secs = g_broker_conf.poll_interval;
		if (!poll_secs)
			poll_secs = SYNC_DEFAULT_POLL_INTERVAL;
		tv.tv_sec  = (long) poll_secs;
		tv.tv_usec = 0;

		FD_ZERO(&rfds);
		if (SYNC_WAKE_RD >= 0)
			FD_SET(SYNC_WAKE_RD, &rfds);
		n = select((SYNC_WAKE_RD >= 0 ? SYNC_WAKE_RD + 1 : 0),
			   (SYNC_WAKE_RD >= 0 ? &rfds : NULL),
			   NULL, NULL, &tv);
		if (n < 0 && errno != EINTR) {
			error("sync_ticker: select: %m");
			break;
		}
		if (n > 0 && SYNC_WAKE_RD >= 0 &&
		    FD_ISSET(SYNC_WAKE_RD, &rfds)) {
			_drain_wakeup_pipe();
			/* loop top will re-check sync_running */
		}
	}
	return NULL;
}

/*****************************************************************************\
 *                       lifecycle
\*****************************************************************************/

int sync_ticker_start(void)
{
	if (sync_thread_active) {
		debug("%s: already started", __func__);
		return SLURM_SUCCESS;
	}

	if (!g_broker_conf.poll_max_retries)
		g_broker_conf.poll_max_retries = SYNC_DEFAULT_POLL_RETRIES;

	if (pipe(sync_wakeup_pipe) < 0) {
		error("sync_ticker: pipe: %m");
		return SLURM_ERROR;
	}
	fd_set_nonblocking(SYNC_WAKE_RD);
	fd_set_nonblocking(SYNC_WAKE_WR);
	fd_set_close_on_exec(SYNC_WAKE_RD);
	fd_set_close_on_exec(SYNC_WAKE_WR);

	consecutive_failures = 0;
	sync_running         = true;
	sync_thread_active   = true;

	slurm_thread_create(&sync_tid, _sync_ticker_main, NULL);
	info("sync_ticker: started (poll_interval=%us, poll_max_retries=%u)",
	     g_broker_conf.poll_interval ?
		     g_broker_conf.poll_interval :
		     SYNC_DEFAULT_POLL_INTERVAL,
	     g_broker_conf.poll_max_retries);
	return SLURM_SUCCESS;
}

void sync_ticker_stop(void)
{
	if (!sync_thread_active)
		return;

	sync_running = false;

	if (SYNC_WAKE_WR >= 0) {
		const char b = 'q';
		ssize_t n;

		do {
			n = write(SYNC_WAKE_WR, &b, 1);
		} while (n < 0 && errno == EINTR);
		if (n < 0 && errno != EAGAIN)
			debug("sync_ticker: wakeup write: %m");
	}

	(void) pthread_join(sync_tid, NULL);
	sync_thread_active = false;

	if (SYNC_WAKE_RD >= 0) {
		(void) close(SYNC_WAKE_RD);
		SYNC_WAKE_RD = -1;
	}
	if (SYNC_WAKE_WR >= 0) {
		(void) close(SYNC_WAKE_WR);
		SYNC_WAKE_WR = -1;
	}

	info("sync_ticker: stopped");
}
