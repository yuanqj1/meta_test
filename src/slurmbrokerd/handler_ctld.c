/*****************************************************************************\
 *  handler_ctld.c - dispatcher and handlers for ctld -> broker inbound RPCs.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See handler_ctld.h for the
 *  contract and doc/checklists/M06-handler-ctld.md for the design.
 *
 *  M06 PR scope
 *  ------------
 *  Implements the two ctld-driven entry points:
 *
 *    REQUEST_FORWARD_JOB   - allocates a broker_job_t in INIT state and
 *                            replies with RESPONSE_FORWARD_JOB carrying
 *                            the assigned trace_id.
 *    REQUEST_BROKER_CANCEL - flips the job's cancel_requested flag; the
 *                            state machine (M09) then propagates the
 *                            cancel both to the local stage worker and
 *                            to the remote broker.
 *
 *  Both handlers are SYNCHRONOUS w.r.t. the wire reply (the ctld agent
 *  thread blocks on slurm_send_recv_node_msg() until we reply). Any
 *  long-running follow-up work (egress, stage, sbatch, scancel) is left
 *  to async modules (M08/M09/M10).
 *
 *  Wire compatibility
 *  ------------------
 *  The 4 ctld<->broker payload structs and msg_type values are tagged
 *  LEGACY_M04_TRANSITIONAL inside proto.{h,c,_pack.c}. As long as the
 *  slurmctld engineer's sister PR registers the same field layout and
 *  the same numeric msg_type values in src/common/, the broker side
 *  can keep using the BROKERD_* macros and `brokerd_forward_job_msg_t`
 *  type-cast view of msg->data with no behavioural difference.
\*****************************************************************************/

#include "config.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/read_config.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "broker_conf.h"
#include "broker_job.h"
#include "handler_ctld.h"
#include "persist.h"
#include "proto.h"
#include "user_mapping.h"

/*****************************************************************************\
 *                       in-flight counter
 *
 * O(N) walk of the job table; acceptable at MVP throughput (<= 500
 * in-flight) because each REQUEST_FORWARD_JOB invokes it exactly once
 * before deciding admission. broker_job_table_foreach() takes
 * g_broker_jobs_lock internally, so the callback must not call back
 * into broker_job_table_*().
\*****************************************************************************/

typedef struct {
	uint32_t inflight;
} _inflight_ctx_t;

static int _count_inflight_cb(broker_job_t *j, void *arg)
{
	_inflight_ctx_t *ctx = arg;

	switch (j->state) {
	case BROKER_STATE_INIT:
	case BROKER_STATE_STAGING_IN:
	case BROKER_STATE_STAGED_IN:
	case BROKER_STATE_SUBMITTED:
	case BROKER_STATE_RUNNING:
	case BROKER_STATE_STAGING_OUT:
		ctx->inflight++;
		break;
	default:
		break;
	}
	return 0; /* keep iterating */
}

static uint32_t _count_inflight(void)
{
	_inflight_ctx_t ctx = { .inflight = 0 };

	broker_job_table_foreach(_count_inflight_cb, &ctx);
	return ctx.inflight;
}

/*****************************************************************************\
 *                       ACL helper
\*****************************************************************************/

/*
 * cancel ACL: only the original submitter, root, or the slurm user
 * (i.e. the ctld process itself) may cancel a broker job. ctld upstream
 * already enforces job-level ACLs but we double-check here so a
 * misbehaving / spoofed agent cannot drive arbitrary cancels through
 * the broker.
 */
static bool _acl_owner_or_root(uint32_t auth_uid, broker_job_t *job)
{
	if (auth_uid == 0)
		return true;
	if (auth_uid == slurm_conf.slurm_user_id)
		return true;
	if (auth_uid == job->src_uid)
		return true;
	return false;
}

/*
 * Forward-job ACL: only root or SlurmUser may push REQUEST_FORWARD_JOB.
 * This RPC must originate from the local slurmctld process (port-bound
 * to 127.0.0.1 by listener.c::_accept_with_acl), which always runs as
 * SlurmUser; anything else is a misconfiguration or attack.
 */
static bool _acl_slurm_user_or_root(uint32_t auth_uid)
{
	return (auth_uid == 0 ||
		auth_uid == slurm_conf.slurm_user_id);
}

/*****************************************************************************\
 *                       reply helpers
 *
 * Two flavours:
 *
 *   _reply_forward_job   - typed RESPONSE_FORWARD_JOB carrying
 *                          (error_code, trace_id). Requires the slurmctld
 *                          PR to have registered the matching
 *                          pack/unpack in src/common/slurm_protocol_pack.c
 *                          so slurm_send_node_msg() can serialize it.
 *
 *   slurm_send_rc_msg    - generic RESPONSE_SLURM_RC fallback used by
 *                          handle_cancel_from_ctld and by all error
 *                          paths where we have no trace_id to report.
\*****************************************************************************/

static void _reply_forward_job(slurm_msg_t *req_msg, uint32_t error_code,
			       const char *trace_id)
{
	brokerd_forward_job_resp_msg_t resp = {
		.error_code = error_code,
		.trace_id   = (char *) (trace_id ? trace_id : ""),
	};
	slurm_msg_t resp_msg;

	slurm_msg_t_init(&resp_msg);
	resp_msg.msg_type         = BROKERD_RESPONSE_FORWARD_JOB;
	resp_msg.protocol_version = req_msg->protocol_version;
	resp_msg.conn_fd          = req_msg->conn_fd;
	resp_msg.address          = req_msg->address;
	resp_msg.flags            = req_msg->flags;
	resp_msg.data             = &resp;

	if (slurm_send_node_msg(req_msg->conn_fd, &resp_msg) < 0) {
		/*
		 * The ctld agent will time out and retry; we already
		 * registered the job in our table, so the retry is safe
		 * (broker_job_table_add will hit the duplicate-trace_id
		 * branch and we'll re-reply with the same trace_id).
		 */
		debug("handle_forward_job: slurm_send_node_msg failed: %m");
	}
}

/*****************************************************************************\
 *                       handle_forward_job (M06-T1)
\*****************************************************************************/

int handle_forward_job(slurm_msg_t *msg)
{
	brokerd_forward_job_msg_t *req = msg->data;
	user_mapping_t *map;
	broker_job_t *job = NULL;
	uint32_t inflight;
	char trace_id[BROKER_TRACE_ID_LEN];

	if (!req) {
		error("handle_forward_job: NULL payload");
		slurm_send_rc_msg(msg, SLURM_ERROR);
		return SLURM_SUCCESS;
	}

	/* 1. ACL: must come from local SlurmUser/root. */
	if (!_acl_slurm_user_or_root(msg->auth_uid)) {
		warning("handle_forward_job: rejecting uid=%u (not SlurmUser/root)",
		        msg->auth_uid);
		_reply_forward_job(msg, ESLURM_USER_ID_MISSING, NULL);
		return SLURM_SUCCESS;
	}

	/* 2. Overload protection: keep <= max_inflight active jobs. */
	inflight = _count_inflight();
	if (g_broker_conf.max_inflight &&
	    inflight >= g_broker_conf.max_inflight) {
		warning("handle_forward_job: rejecting src_job_id=%u, inflight=%u >= max=%u",
		        req->src_job_id, inflight, g_broker_conf.max_inflight);
		_reply_forward_job(msg, BROKERD_ERR_OVERLOAD, NULL);
		return SLURM_SUCCESS;
	}

	/* 3. Resolve user mapping for (local_user, target_cluster). */
	map = user_mapping_lookup(req->src_user_name, req->target_cluster);
	if (!map) {
		error("handle_forward_job: no mapping for user=%s -> %s",
		      req->src_user_name ? req->src_user_name : "(null)",
		      req->target_cluster ? req->target_cluster : "(null)");
		_reply_forward_job(msg, BROKERD_ERR_NO_USER_MAPPING, NULL);
		return SLURM_SUCCESS;
	}

	/* 4. Synthesize trace_id = "<local_cluster>-<src_job_id>" */
	(void) snprintf(trace_id, sizeof(trace_id), "%s-%u",
			g_broker_conf.cluster_name
				? g_broker_conf.cluster_name
				: "broker",
			req->src_job_id);

	/* 5. Allocate broker_job_t and copy the flat fields we need.
	 *
	 * Note: req->job_desc is intentionally NOT consumed. broker_job_t
	 * has no job_desc field anymore (M03 / M05 cleanup); the receiver
	 * broker reconstructs the local job description on its own
	 * libslurm version from (script_path, app_name, partition,
	 * remote_user_name, dst_work_dir). req->job_desc is freed later
	 * by brokerd_free_forward_job_msg() when the listener tears down
	 * the request msg.
	 */
	job = broker_job_create();
	(void) snprintf(job->trace_id, sizeof(job->trace_id), "%s",
			trace_id);
	job->src_job_id       = req->src_job_id;
	job->src_uid          = req->src_uid;
	job->src_user_name    = xstrdup(req->src_user_name);
	job->src_cluster      = xstrdup(g_broker_conf.cluster_name);
	job->dst_cluster      = xstrdup(req->target_cluster);
	job->target_partition = xstrdup(g_broker_conf.default_remote_partition);
	job->src_work_dir     = xstrdup(req->src_work_dir);
	job->script_path      = xstrdup(req->script_path);
	job->app_name         = xstrdup(req->app_name);
	job->account          = xstrdup(req->account);
	job->remote_user_name = xstrdup(map->remote_user);
	job->remote_uid       = map->remote_uid;
	job->remote_gid       = map->remote_gid;
	job->role             = BROKER_ROLE_ORIGINATOR;
	job->hop_count        = 0;
	job->state            = BROKER_STATE_INIT;
	job->state_enter_time = time(NULL);
	job->submit_time      = job->state_enter_time;

	/* 6. Insert into the global table. Duplicate trace_id (e.g. ctld
	 * agent retried after our ACK was lost) is treated as success and
	 * we re-reply with the same trace_id - this is the documented
	 * idempotency contract. */
	if (broker_job_table_add(job) != SLURM_SUCCESS) {
		broker_job_t *existing = broker_job_table_get(trace_id);

		if (existing) {
			info("handle_forward_job: duplicate trace_id=%s, replying SUCCESS (idempotent retry)",
			     trace_id);
			broker_job_destroy(job);
			_reply_forward_job(msg, SLURM_SUCCESS, trace_id);
			return SLURM_SUCCESS;
		}
		error("handle_forward_job: broker_job_table_add failed for trace_id=%s",
		      trace_id);
		broker_job_destroy(job);
		_reply_forward_job(msg, ESLURM_DUPLICATE_JOB_ID, NULL);
		return SLURM_SUCCESS;
	}

	/* 7. Hint persist thread to flush; loss of the in-flight INIT
	 * record across a broker crash would otherwise leave the ctld
	 * with a stranded shadow job. */
	persist_async_request();

	info("handle_forward_job: trace_id=%s src_job_id=%u user=%s -> %s/%s, app=%s",
	     job->trace_id, job->src_job_id, job->src_user_name,
	     job->dst_cluster, job->target_partition,
	     job->app_name ? job->app_name : "(none)");

	/* 8. ACK with the trace_id so the ctld agent can correlate. */
	_reply_forward_job(msg, SLURM_SUCCESS, job->trace_id);
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       handle_cancel_from_ctld (M06-T2)
\*****************************************************************************/

int handle_cancel_from_ctld(slurm_msg_t *msg)
{
	brokerd_broker_cancel_msg_t *req = msg->data;
	broker_job_t *job;
	char trace_id[BROKER_TRACE_ID_LEN];

	if (!req) {
		error("handle_cancel_from_ctld: NULL payload");
		slurm_send_rc_msg(msg, SLURM_ERROR);
		return SLURM_SUCCESS;
	}

	/*
	 * ctld populates `src_job_id` (its local job id); broker derives
	 * the trace_id locally. The `trace_id` field on the wire is
	 * reserved for the broker -> broker propagation case (handler_remote)
	 * and should be empty/NULL when sourced from ctld.
	 */
	if (!req->src_job_id) {
		error("handle_cancel_from_ctld: missing src_job_id");
		slurm_send_rc_msg(msg, ESLURM_INVALID_JOB_ID);
		return SLURM_SUCCESS;
	}

	(void) snprintf(trace_id, sizeof(trace_id), "%s-%u",
			g_broker_conf.cluster_name
				? g_broker_conf.cluster_name
				: "broker",
			req->src_job_id);

	job = broker_job_table_get(trace_id);
	if (!job) {
		debug("handle_cancel_from_ctld: trace_id=%s not found",
		      trace_id);
		/* Not an error: ctld may scancel after the broker job
		 * already terminated and was reaped. Reply not-found so
		 * the agent stops retrying. */
		slurm_send_rc_msg(msg, BROKERD_ERR_NOT_FOUND);
		return SLURM_SUCCESS;
	}

	/* ACL: owner / root / SlurmUser only. */
	if (!_acl_owner_or_root(msg->auth_uid, job)) {
		warning("handle_cancel_from_ctld: uid=%u rejected for trace_id=%s (owner_uid=%u)",
		        msg->auth_uid, trace_id, job->src_uid);
		slurm_send_rc_msg(msg, ESLURM_USER_ID_MISSING);
		return SLURM_SUCCESS;
	}

	slurm_mutex_lock(&job->lock);
	if (job->cancel_requested) {
		slurm_mutex_unlock(&job->lock);
		debug("handle_cancel_from_ctld: trace_id=%s already cancel_requested",
		      trace_id);
	} else {
		job->cancel_requested = true;
		slurm_mutex_unlock(&job->lock);
		info("handle_cancel_from_ctld: trace_id=%s scheduled for cancel",
		     trace_id);
	}

	/*
	 * Force a flush so the cancel intent survives a broker crash
	 * before the next 30s checkpoint tick.
	 */
	persist_async_request();

	slurm_send_rc_msg(msg, SLURM_SUCCESS);
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       dispatcher
\*****************************************************************************/

void dispatch_ctld_msg(slurm_msg_t *msg)
{
	char addr_str[INET6_ADDRSTRLEN] = "";

	slurm_get_ip_str(&msg->address, addr_str, sizeof(addr_str));
	debug2("dispatch_ctld_msg: msg_type=%u from %s uid=%u",
	       msg->msg_type, addr_str, msg->auth_uid);

	/*
	 * msg->msg_type is whatever the slurm-native unpack_msg() decoded.
	 * The broker keeps its own BROKERD_REQUEST_* macros (proto.h) whose
	 * numeric values match the slurmctld-side registrations once that
	 * PR lands; the cases below catch both because the macros expand to
	 * the same uint16_t literal.
	 */
	switch (msg->msg_type) {
	case BROKERD_REQUEST_FORWARD_JOB:
		(void) handle_forward_job(msg);
		break;
	case BROKERD_REQUEST_BROKER_CANCEL:
		(void) handle_cancel_from_ctld(msg);
		break;
	default:
		error("dispatch_ctld_msg: unsupported msg_type=%u from %s",
		      msg->msg_type, addr_str);
		slurm_send_rc_msg(msg, SLURM_UNEXPECTED_MSG_ERROR);
		break;
	}
}
