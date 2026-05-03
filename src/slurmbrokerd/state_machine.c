/*****************************************************************************\
 *  state_machine.c - 1Hz tick that drives broker_job state progression.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See state_machine.h for
 *  the contract and doc/checklists/M09-state-machine.md for the design.
 *
 *  Two-phase tick to avoid holding g_broker_jobs_lock across blocking
 *  egress calls; see state_machine.h "Threading & locking" for details.
 *
 *  Wakeup pipe (same self-pipe trick as listener.c) lets stop() unblock
 *  the sleep within ~1ms instead of waiting up to a full second; the
 *  1s sleep timeout is still there as belt-and-suspenders.
\*****************************************************************************/

#include "config.h"

#include <errno.h>
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
#include "src/common/list.h"
#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "broker_conf.h"
#include "broker_job.h"
#include "egress.h"
#include "persist.h"
#include "state_machine.h"

/*****************************************************************************\
 *                       module-local state
\*****************************************************************************/

static pthread_t sm_tid;
static volatile bool sm_running = false;
static bool          sm_thread_active = false;

/* Self-pipe for instant shutdown wakeup. See listener.c for the
 * rationale (close-fd-during-select race etc). */
static int sm_wakeup_pipe[2] = { -1, -1 };
#define SM_WAKE_RD  sm_wakeup_pipe[0]
#define SM_WAKE_WR  sm_wakeup_pipe[1]

/* Tick period. */
#define SM_TICK_SECS  1

/* Per-state thresholds (M09 §5). */
#define SM_INIT_TIMEOUT_S          60
#define SM_STAGED_IN_RETRY_INT_S   30
#define SM_SUBMITTED_WATCHDOG_S    (24 * 60 * 60)  /* 24h */
#define SM_MAX_RETRIES             3

/* MVP placeholder: real estimate comes from M10-T4 du -sb. */
#define SM_DEFAULT_DATA_SIZE_GB    1

/*****************************************************************************\
 *                       per-tick action queue
 *
 * Phase 1 (under g_broker_jobs_lock) emits SM_ACTION_* descriptors;
 * Phase 2 (lock released) drains and executes them. Actions are
 * keyed by trace_id (not by broker_job_t pointer) so phase 2 can
 * safely re-resolve via broker_job_table_get() and skip jobs that
 * have already been removed by other code paths between phases.
\*****************************************************************************/

typedef enum {
	SM_ACTION_NONE = 0,
	SM_ACTION_FORWARD,           /* INIT(ORIG) -> egress_forward_async */
	SM_ACTION_RESEND_STAGED_IN,  /* STAGED_IN retry -> egress_staged_in_async */
	SM_ACTION_CANCEL_REMOTE,     /* propagate scancel to peer */
	SM_ACTION_INJECT_TERMINAL,   /* terminal -> ctld_inject_terminal_state */
} sm_action_type_t;

typedef struct {
	sm_action_type_t type;
	char trace_id[BROKER_TRACE_ID_LEN];
} sm_action_t;

/* trailing list of trace_id strings to broker_job_table_remove() in
 * phase 3 (after phase 2 RPCs have returned). */
typedef struct {
	char trace_id[BROKER_TRACE_ID_LEN];
} sm_remove_t;

typedef struct {
	list_t *actions;  /* element type sm_action_t* (xmalloc'd)  */
	list_t *removes;  /* element type sm_remove_t* (xmalloc'd) */
	time_t  now;
} sm_tick_ctx_t;

static void _action_free(void *p)   { xfree(p); }
static void _remove_free(void *p)   { xfree(p); }

static void _enqueue_action(sm_tick_ctx_t *ctx, sm_action_type_t type,
			    const char *trace_id)
{
	sm_action_t *a = xmalloc(sizeof(*a));

	a->type = type;
	(void) snprintf(a->trace_id, sizeof(a->trace_id), "%s",
			trace_id ? trace_id : "");
	list_append(ctx->actions, a);
}

static void _enqueue_remove(sm_tick_ctx_t *ctx, const char *trace_id)
{
	sm_remove_t *r = xmalloc(sizeof(*r));

	(void) snprintf(r->trace_id, sizeof(r->trace_id), "%s",
			trace_id ? trace_id : "");
	list_append(ctx->removes, r);
}

/*****************************************************************************\
 *                       transition (public)
\*****************************************************************************/

void state_machine_transition(broker_job_t *job, broker_job_state_t to,
			      const char *reason)
{
	broker_job_state_t from;

	if (!job)
		return;

	slurm_mutex_lock(&job->lock);
	from = job->state;
	if (from == to) {
		slurm_mutex_unlock(&job->lock);
		return;
	}
	job->state            = to;
	job->state_enter_time = time(NULL);
	xfree(job->state_reason);
	if (reason && *reason)
		job->state_reason = xstrdup(reason);
	slurm_mutex_unlock(&job->lock);

	info("transition: trace_id=%s state %d -> %d%s%s",
	     job->trace_id, (int) from, (int) to,
	     reason ? " reason=" : "", reason ? reason : "");

	persist_async_request();
}

/*****************************************************************************\
 *                       per-state helpers (Phase 1, under table lock)
 *
 * Each helper inspects the job, possibly mutates state via local helpers
 * (broker_job_set_state etc), and may enqueue an action for Phase 2.
 * They MUST NOT call broker_job_table_* (would self-deadlock the
 * foreach iterator) and MUST NOT call egress_* (would block the lock).
\*****************************************************************************/

static int _stage_timeout_secs(void)
{
	uint32_t per_gb = g_broker_conf.stage_timeout_per_gb;

	/* per_gb is uint32; default to 120 s/GB if config is unset (e.g.
	 * during early boot before broker.conf parse completes). */
	if (!per_gb)
		per_gb = 120;
	return (int) (SM_DEFAULT_DATA_SIZE_GB * per_gb + 600);
}

static void _on_init(broker_job_t *job, sm_tick_ctx_t *ctx)
{
	if (job->role == BROKER_ROLE_ORIGINATOR) {
		/* Trigger the cross-cluster forward; egress will transition
		 * the job to STAGING_IN on success or FAILED on failure
		 * (M08 sets state internally). */
		_enqueue_action(ctx, SM_ACTION_FORWARD, job->trace_id);
		return;
	}

	/* RECEIVER side: INIT means "broker_forward_job handler finished
	 * mkdir; awaiting STAGED_IN from originator". A 60s drought
	 * indicates the originator side died. */
	if ((ctx->now - job->state_enter_time) > SM_INIT_TIMEOUT_S) {
		state_machine_transition(job, BROKER_STATE_FAILED,
					 "INIT timeout (no STAGED_IN)");
	}
}

static void _on_staging_in(broker_job_t *job, sm_tick_ctx_t *ctx)
{
	if ((ctx->now - job->state_enter_time) < _stage_timeout_secs())
		return;

	/* TODO M10-T1: when stage_pool lands, this branch may also want
	 * to inspect stage worker progress instead of relying solely on
	 * a timeout. */
	if (job->retry_count < SM_MAX_RETRIES) {
		slurm_mutex_lock(&job->lock);
		job->retry_count++;
		slurm_mutex_unlock(&job->lock);
		state_machine_transition(job, BROKER_STATE_INIT,
					 "stage_in retry");
	} else {
		state_machine_transition(job, BROKER_STATE_FAILED,
					 "stage_in timeout");
	}
}

static void _on_staged_in(broker_job_t *job, sm_tick_ctx_t *ctx)
{
	if ((ctx->now - job->state_enter_time) < SM_STAGED_IN_RETRY_INT_S)
		return;

	if (job->retry_count < SM_MAX_RETRIES) {
		slurm_mutex_lock(&job->lock);
		job->retry_count++;
		job->state_enter_time = ctx->now; /* reset retry window */
		slurm_mutex_unlock(&job->lock);
		_enqueue_action(ctx, SM_ACTION_RESEND_STAGED_IN,
				job->trace_id);
	} else {
		state_machine_transition(job, BROKER_STATE_FAILED,
					 "staged_in retries exhausted");
	}
}

static void _on_submitted(broker_job_t *job, sm_tick_ctx_t *ctx)
{
	/* sync_ticker (M13) is responsible for advancing SUBMITTED ->
	 * RUNNING. Here we only enforce the 24h pending watchdog. */
	if ((ctx->now - job->state_enter_time) > SM_SUBMITTED_WATCHDOG_S) {
		state_machine_transition(job, BROKER_STATE_FAILED,
					 "remote pending too long (24h)");
	}
}

static void _on_running(broker_job_t *job, sm_tick_ctx_t *ctx)
{
	/* No soft timeout: trust the remote ctld's TimeLimit. sync_ticker
	 * advances RUNNING -> STAGING_OUT when the remote job completes. */
	(void) job;
	(void) ctx;
}

static void _on_staging_out(broker_job_t *job, sm_tick_ctx_t *ctx)
{
	if ((ctx->now - job->state_enter_time) < _stage_timeout_secs())
		return;

	/* TODO M10-T2: re-trigger stage_pool reverse rsync. For MVP we
	 * just move to FAILED after retries are exhausted; per design doc
	 * §5.1 the dst_work_dir is retained for 7d for ops debug. */
	if (job->retry_count < SM_MAX_RETRIES) {
		slurm_mutex_lock(&job->lock);
		job->retry_count++;
		job->state_enter_time = ctx->now;
		slurm_mutex_unlock(&job->lock);
		debug("stage_out retry trace_id=%s attempt=%u",
		      job->trace_id, job->retry_count);
	} else {
		state_machine_transition(job, BROKER_STATE_FAILED,
					 "stage_out retries exhausted");
	}
}

static void _on_terminal(broker_job_t *job, sm_tick_ctx_t *ctx)
{
	/*
	 * Terminal jobs:
	 *   ORIGINATOR -> push the terminal state to the local ctld so
	 *                 the shadow job transitions out of PENDING(Held)
	 *                 and gets a sacct record.
	 *   RECEIVER  -> nothing to push; the cleanup below removes the
	 *                table entry. The dst_work_dir survives until
	 *                M14 cleanup reaper sweeps it (per retention).
	 *
	 * Both roles get the row removed from the table this tick. M14
	 * is responsible for delayed remote-side cleanup; we deliberately
	 * do NOT call egress_cleanup_async() here so failed jobs keep
	 * their dst_work_dir for ops debug.
	 */
	if (job->role == BROKER_ROLE_ORIGINATOR) {
		_enqueue_action(ctx, SM_ACTION_INJECT_TERMINAL,
				job->trace_id);
	}
	_enqueue_remove(ctx, job->trace_id);
}

/*****************************************************************************\
 *                       Phase 1: tick callback (under table lock)
\*****************************************************************************/

static int _tick_one(broker_job_t *job, void *arg)
{
	sm_tick_ctx_t *ctx = arg;

	/* Cancel takes priority over every state branch -- cancel propagation
	 * happens at most once (cancel_propagated guards re-entry). */
	if (job->cancel_requested && !job->cancel_propagated) {
		switch (job->state) {
		case BROKER_STATE_COMPLETED:
		case BROKER_STATE_FAILED:
		case BROKER_STATE_CANCELLED:
			/* terminal already, cancel is moot */
			break;
		default:
			_enqueue_action(ctx, SM_ACTION_CANCEL_REMOTE,
					job->trace_id);
			state_machine_transition(job, BROKER_STATE_CANCELLED,
						 "user cancelled");
			/* fall through to terminal handling next tick */
			return 0;
		}
	}

	switch (job->state) {
	case BROKER_STATE_INIT:
		_on_init(job, ctx);
		break;
	case BROKER_STATE_STAGING_IN:
		_on_staging_in(job, ctx);
		break;
	case BROKER_STATE_STAGED_IN:
		_on_staged_in(job, ctx);
		break;
	case BROKER_STATE_SUBMITTED:
		_on_submitted(job, ctx);
		break;
	case BROKER_STATE_RUNNING:
		_on_running(job, ctx);
		break;
	case BROKER_STATE_STAGING_OUT:
		_on_staging_out(job, ctx);
		break;
	case BROKER_STATE_COMPLETED:
	case BROKER_STATE_FAILED:
	case BROKER_STATE_CANCELLED:
		_on_terminal(job, ctx);
		break;
	default:
		warning("state_machine: trace_id=%s unknown state %d",
		        job->trace_id, (int) job->state);
		break;
	}
	return 0;
}

/*****************************************************************************\
 *                       Phase 2: drain the action queue (lock released)
\*****************************************************************************/

static void _execute_actions(list_t *actions)
{
	list_itr_t *itr;
	sm_action_t *a;

	if (!actions || !list_count(actions))
		return;

	itr = list_iterator_create(actions);
	while ((a = list_next(itr))) {
		broker_job_t *job;

		/* Re-resolve: between phase 1 and now, another thread
		 * could have removed the job (rare). Skip silently. */
		job = broker_job_table_get(a->trace_id);
		if (!job) {
			debug2("state_machine: action %d for trace_id=%s no longer in table",
			       (int) a->type, a->trace_id);
			continue;
		}

		switch (a->type) {
		case SM_ACTION_FORWARD:
			(void) egress_forward_async(job);
			break;
		case SM_ACTION_RESEND_STAGED_IN:
			(void) egress_staged_in_async(job);
			break;
		case SM_ACTION_CANCEL_REMOTE:
			(void) egress_cancel_async(job);
			break;
		case SM_ACTION_INJECT_TERMINAL:
			(void) ctld_inject_terminal_state(job);
			break;
		default:
			warning("state_machine: ignoring unknown action type %d",
			        (int) a->type);
			break;
		}
	}
	list_iterator_destroy(itr);
	list_flush(actions);
}

/*****************************************************************************\
 *                       Phase 3: terminal removals
\*****************************************************************************/

static void _execute_removes(list_t *removes)
{
	list_itr_t *itr;
	sm_remove_t *r;

	if (!removes || !list_count(removes))
		return;

	itr = list_iterator_create(removes);
	while ((r = list_next(itr))) {
		(void) broker_job_table_remove(r->trace_id);
		debug2("state_machine: removed trace_id=%s", r->trace_id);
	}
	list_iterator_destroy(itr);
	list_flush(removes);
}

/*****************************************************************************\
 *                       tick driver
\*****************************************************************************/

static void _drain_wakeup_pipe(void)
{
	char tmp[64];
	ssize_t n;

	do {
		n = read(SM_WAKE_RD, tmp, sizeof(tmp));
	} while (n > 0 || (n < 0 && errno == EINTR));
}

static void _do_one_tick(sm_tick_ctx_t *ctx)
{
	ctx->now = time(NULL);

	/* Phase 1: under g_broker_jobs_lock, collect actions. */
	broker_job_table_foreach(_tick_one, ctx);

	/* Phase 2: lock released, execute egress / ctld pushes. */
	_execute_actions(ctx->actions);

	/* Phase 3: terminal removals. broker_job_table_remove handles
	 * its own locking. */
	_execute_removes(ctx->removes);
}

static void *_state_machine_thread(void *arg)
{
	sm_tick_ctx_t ctx;

	(void) arg;

	ctx.actions = list_create(_action_free);
	ctx.removes = list_create(_remove_free);
	ctx.now     = time(NULL);

	while (sm_running) {
		fd_set rfds;
		struct timeval tv = { .tv_sec = SM_TICK_SECS, .tv_usec = 0 };
		int n;

		_do_one_tick(&ctx);
		if (!sm_running)
			break;

		/* Sleep up to SM_TICK_SECS, wakeable via the self-pipe. */
		FD_ZERO(&rfds);
		if (SM_WAKE_RD >= 0)
			FD_SET(SM_WAKE_RD, &rfds);
		n = select((SM_WAKE_RD >= 0 ? SM_WAKE_RD + 1 : 0),
			   (SM_WAKE_RD >= 0 ? &rfds : NULL),
			   NULL, NULL, &tv);
		if (n < 0 && errno != EINTR) {
			error("state_machine: select: %m");
			break;
		}
		if (n > 0 && SM_WAKE_RD >= 0 &&
		    FD_ISSET(SM_WAKE_RD, &rfds)) {
			_drain_wakeup_pipe();
			/* loop top will re-check sm_running */
		}
	}

	FREE_NULL_LIST(ctx.actions);
	FREE_NULL_LIST(ctx.removes);
	return NULL;
}

/*****************************************************************************\
 *                       lifecycle
\*****************************************************************************/

int state_machine_start(void)
{
	if (sm_thread_active) {
		debug("%s: already started", __func__);
		return SLURM_SUCCESS;
	}

	if (pipe(sm_wakeup_pipe) < 0) {
		error("state_machine: pipe: %m");
		return SLURM_ERROR;
	}
	fd_set_nonblocking(SM_WAKE_RD);
	fd_set_nonblocking(SM_WAKE_WR);
	fd_set_close_on_exec(SM_WAKE_RD);
	fd_set_close_on_exec(SM_WAKE_WR);

	sm_running       = true;
	sm_thread_active = true;

	slurm_thread_create(&sm_tid, _state_machine_thread, NULL);
	info("state_machine: tick thread started (period=%us)", SM_TICK_SECS);
	return SLURM_SUCCESS;
}

void state_machine_stop(void)
{
	if (!sm_thread_active)
		return;

	sm_running = false;

	/* Self-pipe wakeup: write a single byte so the in-flight select()
	 * returns immediately instead of waiting up to 1s. */
	if (SM_WAKE_WR >= 0) {
		const char b = 'q';
		ssize_t n;

		do {
			n = write(SM_WAKE_WR, &b, 1);
		} while (n < 0 && errno == EINTR);
		if (n < 0 && errno != EAGAIN)
			debug("state_machine: wakeup write: %m");
	}

	(void) pthread_join(sm_tid, NULL);
	sm_thread_active = false;

	if (SM_WAKE_RD >= 0) {
		(void) close(SM_WAKE_RD);
		SM_WAKE_RD = -1;
	}
	if (SM_WAKE_WR >= 0) {
		(void) close(SM_WAKE_WR);
		SM_WAKE_WR = -1;
	}

	info("state_machine: tick thread stopped");
}
