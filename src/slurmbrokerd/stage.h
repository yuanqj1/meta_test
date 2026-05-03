/*****************************************************************************\
 *  stage.h - asynchronous data-staging worker pool (rsync over SSH).
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/checklists/M10-stage.md
 *  and doc/Broker详细设计文档MVP.md §9.
 *
 *  Responsibilities
 *  ----------------
 *  Owns a fixed-size pthread worker pool that pops jobs off two FIFO
 *  queues (stage-in / stage-out) and runs `sudo + rsync` to move data:
 *
 *    stage-in   : src host  -- rsync over ssh -->  remote broker host
 *                 (push <src_work_dir>/ to <remote_broker_host>:<dst_work_dir>/)
 *    stage-out  : remote host -- rsync over ssh -->  src host
 *                 (pull <remote_broker_host>:<dst_work_dir>/ back to <src_work_dir>/)
 *
 *  Worker semantics
 *  ----------------
 *  - submit_in/out enqueue and signal the cond; caller does NOT block.
 *  - A worker pops one task, fork+exec()'s sudo+rsync (stdout/stderr
 *    redirected to /var/log/slurm/broker_stage/<trace_id>.log) and
 *    waitpid()'s. A safety timeout (default 1h, see STAGE_CHILD_TIMEOUT_S)
 *    kills runaway children with SIGKILL so the worker can recycle.
 *  - On exit-code 0 the worker calls state_machine_transition to the
 *    next state (STAGED_IN for stage-in, COMPLETED for stage-out) and,
 *    only for stage-in, fires egress_staged_in_async() to tell the
 *    receiver broker the script is ready to sbatch.
 *  - On non-zero exit the worker logs and lets M09 _on_staging_in /
 *    _on_staging_out's timeout-and-retry path drive the next attempt.
 *
 *  Slurm-version independence
 *  --------------------------
 *  This module shells out to /usr/bin/rsync and /usr/bin/sudo; no
 *  libslurm RPCs are touched, so cross-cluster heterogeneous Slurm
 *  versions are irrelevant here.
\*****************************************************************************/

#ifndef _BROKERD_STAGE_H
#define _BROKERD_STAGE_H

#include "broker_job.h"

/*****************************************************************************\
 *                       lifecycle
\*****************************************************************************/

/*
 * Spawn the configured number of worker pthreads (default 4 if
 * g_broker_conf.stage_worker_count is zero). Idempotent. Must be
 * called AFTER egress_init() (workers call egress_staged_in_async)
 * and AFTER state_machine_start() (workers call
 * state_machine_transition). Conventionally started BEFORE
 * sync_ticker / listener so the queues are live when the first
 * stage_submit_* arrives.
 */
extern int  stage_pool_start(void);

/*
 * Signal all workers to exit, broadcast the queue cond so any sleeping
 * worker returns immediately, then join. Workers currently inside
 * waitpid(rsync) will not exit until the rsync child returns or hits
 * its safety timeout; shutdown latency is therefore bounded by
 * STAGE_CHILD_TIMEOUT_S in the worst case.
 */
extern void stage_pool_stop(void);

/*****************************************************************************\
 *                       submission
 *
 * Both submit functions are non-blocking enqueue + cond_signal. They
 * borrow the broker_job_t pointer; ownership stays with the global
 * job table. Callers MUST guarantee the broker_job_t will not be
 * removed from the table until the worker has finished the task --
 * in practice this is ensured because broker_job_table_remove only
 * runs from terminal-state cleanup in state_machine, which by then
 * has already left STAGING_IN / STAGING_OUT.
\*****************************************************************************/

/*
 * Enqueue a stage-in (src -> remote) for `job`. Performs a `du -sb`
 * pre-flight check; if the size exceeds g_broker_conf.max_stage_bytes
 * the job is transitioned to BROKER_STATE_FAILED and SLURM_ERROR is
 * returned without queueing.
 */
extern int stage_submit_in(broker_job_t *job);

/*
 * Enqueue a stage-out (remote -> src) for `job`. No size pre-flight;
 * the remote side controls how much output the job produced.
 */
extern int stage_submit_out(broker_job_t *job);

#endif /* _BROKERD_STAGE_H */
