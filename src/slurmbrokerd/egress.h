/*****************************************************************************\
 *  egress.h - outbound RPC wrappers driven by the broker state machine,
 *             sync_ticker and cleanup paths.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/checklists/M08-egress.md
 *  and doc/Broker详细设计文档MVP.md §6.3.
 *
 *  This module is the single funnel for every outbound RPC the broker
 *  initiates. It splits cleanly into two channels so that callers do
 *  not have to think about wire format or auth at all:
 *
 *  ╭──────────────────────────────╮     ╭──────────────────────────────╮
 *  │  broker -> broker (5 RPCs)   │     │  broker -> ctld (2 RPCs)     │
 *  │  egress_forward_async        │     │  ctld_update_remote_state    │
 *  │  egress_staged_in_async      │     │  ctld_inject_terminal_state  │
 *  │  egress_query_status_sync    │     │                              │
 *  │  egress_cancel_async         │     │  goes through slurm-native   │
 *  │  egress_cleanup_async        │     │  slurm_send_recv_controller  │
 *  │  uses proto_send_*_to_peer    │    │  _rc_msg(); requires the     │
 *  │  (broker private wire frame)  │    │  slurmctld engineer's PR to  │
 *  │  - PERMANENT under M04 §10    │    │  register the matching       │
 *  │                               │    │  msg_type / pack/unpack in    │
 *  │                               │    │  src/common/                  │
 *  ╰──────────────────────────────╯     ╰──────────────────────────────╯
 *
 *  The "_async" suffix is a naming carry-over from the design doc; in
 *  the MVP every wrapper is in fact synchronous-blocking on the calling
 *  thread, with a built-in retry-with-backoff helper. Future async
 *  refactor (worker pool, io_uring) is left to v0.2.
\*****************************************************************************/

#ifndef _BROKERD_EGRESS_H
#define _BROKERD_EGRESS_H

#include <inttypes.h>

#include "broker_job.h"
#include "proto.h"

/*****************************************************************************\
 *                       lifecycle
\*****************************************************************************/

/*
 * No background threads or sockets of our own; this is just a sanity
 * check that proto_init() and slurm_init() have already run, plus a
 * place to plumb future per-thread context. Always SLURM_SUCCESS today.
 */
extern int  egress_init(void);
extern void egress_fini(void);

/*****************************************************************************\
 *                       broker -> broker (cross-cluster)
 *
 * Each wrapper transitions broker_job state on success/failure, calls
 * persist_async_request() so the change survives a crash, and returns
 * SLURM_SUCCESS / SLURM_ERROR / BROKERD_ERR_* per the M04 error model.
 *
 * IN job  - broker_job_t to source the request fields from. The wrapper
 *           reads multiple fields under broker-flat semantics; do not
 *           hold job->lock across the call (the wrapper takes it itself
 *           when mutating state). Caller retains ownership.
\*****************************************************************************/

extern int egress_forward_async(broker_job_t *job);

extern int egress_staged_in_async(broker_job_t *job);

/*
 * Synchronous batched query. resp_out is a fresh xmalloc'd
 * brokerd_broker_status_msg_t; caller must release with
 * brokerd_free_broker_status_msg().
 */
extern int egress_query_status_sync(char **trace_ids, uint32_t n,
				    brokerd_broker_status_msg_t **resp_out);

extern int egress_cancel_async(broker_job_t *job);

/*
 * Cleanup is keyed by trace_id alone (broker_job_t may already be
 * gone from the local table when cleanup runs).
 */
extern int egress_cleanup_async(const char *trace_id);

/*****************************************************************************\
 *                       broker -> local ctld
\*****************************************************************************/

extern int ctld_update_remote_state(broker_job_t *job);

extern int ctld_inject_terminal_state(broker_job_t *job);

#endif /* _BROKERD_EGRESS_H */
