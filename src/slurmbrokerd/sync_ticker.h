/*****************************************************************************\
 *  sync_ticker.h - ORIGINATOR-side periodic remote-state poller.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/checklists/M13-sync-ticker.md
 *  and doc/Broker详细设计文档MVP.md §6.2 / §6.3.
 *
 *  Responsibilities
 *  ----------------
 *  - Spawns a single background thread that wakes every
 *    g_broker_conf.poll_interval seconds (default 10).
 *  - Each tick walks the global broker_jobs table, collects every
 *    ORIGINATOR-side job that is SUBMITTED or RUNNING, and issues ONE
 *    REQUEST_BROKER_QUERY_STATUS to the peer broker carrying every
 *    trace_id at once (batched). The peer responds with one
 *    RESPONSE_BROKER_STATUS containing N entries.
 *  - For each entry, updates the local broker_job's remote_*
 *    bookkeeping fields, transitions broker state per the mapping
 *    table in M13 §2.4, and pushes ctld_update_remote_state() so the
 *    user's `squeue --remote` shows fresh values.
 *  - JOB_COMPLETE / JOB_FAILED / JOB_TIMEOUT triggers transition to
 *    BROKER_STATE_STAGING_OUT (M10 stage_pool will then handle the
 *    reverse rsync).
 *
 *  Threading & locking
 *  -------------------
 *  Three-phase tick (same pattern as state_machine.c):
 *
 *    Phase 1 (g_broker_jobs_lock held): foreach -> _collect_trace_id
 *            walks the table, copies trace_ids into a local array.
 *    Phase 2 (lock released): egress_query_status_sync() blocks on
 *            the network round-trip to the peer broker (~30s worst).
 *    Phase 3 (lock released): per-entry _apply_remote_status() takes
 *            short-lived locks (broker_job_table_get + job->lock) to
 *            write fields, then transitions via state_machine_transition
 *            and pushes via ctld_update_remote_state.
 *
 *  Failure handling
 *  ----------------
 *  Transient query failures bump consecutive_failures; once the count
 *  reaches g_broker_conf.poll_max_retries we escalate from warning()
 *  to error() but do NOT stop the thread or mutate any broker_job
 *  state -- the M09 SUBMITTED 24h watchdog backstops a permanently
 *  unreachable peer.
\*****************************************************************************/

#ifndef _BROKERD_SYNC_TICKER_H
#define _BROKERD_SYNC_TICKER_H

/*
 * Spawn the poller thread. Idempotent. Returns SLURM_SUCCESS or
 * SLURM_ERROR if the wakeup pipe / thread cannot be set up.
 *
 * Must be called AFTER egress_init() (egress_query_status_sync is the
 * RPC primitive) and AFTER state_machine_start() (apply path drives
 * state_machine_transition).
 */
extern int  sync_ticker_start(void);

/*
 * Signal the poller to exit, write 1 byte to the wakeup pipe so the
 * 10s sleep returns immediately, then join. Must be called BEFORE
 * egress_fini() so the in-flight tick can complete cleanly.
 */
extern void sync_ticker_stop(void);

#endif /* _BROKERD_SYNC_TICKER_H */
