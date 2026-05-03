/*****************************************************************************\
 *  state_machine.h - 1Hz tick that drives broker_job state progression.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/checklists/M09-state-machine.md
 *  and doc/Broker详细设计文档MVP.md §5.
 *
 *  Responsibilities
 *  ----------------
 *  - Single background thread, 1s tick.
 *  - Per-job branch on broker_job_t.state, applying timeouts, retries,
 *    and triggering egress wrappers (M08) for ORIGINATOR-side
 *    progression (INIT -> STAGING_IN -> STAGED_IN -> SUBMITTED).
 *  - Cancel priority: cancel_requested && !cancel_propagated triggers
 *    egress_cancel_async + transition CANCELLED before any state
 *    branch runs.
 *  - Terminal states (COMPLETED / FAILED / CANCELLED) get one
 *    ctld_inject_terminal_state push (ORIGINATOR only) followed by
 *    table removal in the same tick.
 *
 *  Threading & locking
 *  -------------------
 *  Tick body uses a TWO-PHASE pattern to never hold
 *  g_broker_jobs_lock across a blocking egress / RPC call:
 *
 *    Phase 1 (lock held): broker_job_table_foreach() walks the table,
 *                         the per-job callback inspects state and
 *                         appends a small "action descriptor" struct
 *                         (trace_id + action type) to a per-tick
 *                         in-memory queue. State mutations that need
 *                         only job->lock (e.g. timeout -> FAILED) are
 *                         performed inline because they are O(microseconds).
 *
 *    Phase 2 (lock released): action queue is drained; each entry
 *                             re-resolves its trace_id with
 *                             broker_job_table_get() and invokes the
 *                             matching egress wrapper / ctld push.
 *                             A trailing pass calls
 *                             broker_job_table_remove() for jobs
 *                             flagged for terminal cleanup.
 *
 *  Anything in M07/M08 may call state_machine_transition() to record a
 *  state change with reason + bumped state_enter_time + persist hint;
 *  the call is safe from any thread.
\*****************************************************************************/

#ifndef _BROKERD_STATE_MACHINE_H
#define _BROKERD_STATE_MACHINE_H

#include "broker_job.h"

/*****************************************************************************\
 *                       lifecycle
\*****************************************************************************/

/*
 * Spawn the 1Hz tick thread. Idempotent. Returns SLURM_SUCCESS on
 * success, SLURM_ERROR if the wakeup pipe / thread cannot be set up.
 *
 * Must be called AFTER broker_job_table_init() / persist_thread_start()
 * / proto_init() / egress_init() so that the tick body has every
 * downstream dependency live when its first iteration fires.
 */
extern int  state_machine_start(void);

/*
 * Signal the tick thread to exit, write 1 byte to the wakeup pipe so
 * the current sleep returns immediately, and join. Must be called
 * BEFORE listener_stop() is reverted? No -- listener_stop() must come
 * first (per slurmbrokerd_fini ordering) so the tick thread is the one
 * holding the table when no fresh inbound RPC can drive new entries.
 */
extern void state_machine_stop(void);

/*****************************************************************************\
 *                       transition (callable from any thread)
\*****************************************************************************/

/*
 * Atomic state change with side effects:
 *   - holds job->lock for the duration
 *   - no-op if job->state == to (idempotent on retry / race)
 *   - bumps job->state_enter_time
 *   - replaces job->state_reason (reason==NULL clears, otherwise
 *     xstrdup'd; caller keeps ownership of input)
 *   - calls persist_async_request() so the change survives a crash
 *     before the next 30s checkpoint
 *   - emits an info() log line so transitions are grep-able
 *
 * This is a thin superset of broker_job_set_state() (M07) -- M09
 * callers should prefer state_machine_transition() because it logs and
 * persists. Existing M07/M08 code calls broker_job_set_state() directly
 * (and pairs it with its own persist_async_request); both APIs converge
 * on the same locking model.
 */
extern void state_machine_transition(broker_job_t *job,
				     broker_job_state_t to,
				     const char *reason);

#endif /* _BROKERD_STATE_MACHINE_H */
