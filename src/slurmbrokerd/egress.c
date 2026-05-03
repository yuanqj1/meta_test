/*****************************************************************************\
 *  egress.c - outbound RPC wrappers (broker -> broker, broker -> ctld).
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See egress.h for the
 *  contract and doc/checklists/M08-egress.md for the design.
 *
 *  Wire format split (from M04 §10):
 *
 *    broker -> broker  (5 RPCs):
 *        proto_send_recv_to_peer  / proto_send_to_peer
 *        - broker private wire frame ('BRKR' magic + auth + payload)
 *        - delivered to PeerPort 8443 of the configured remote broker
 *        - PERMANENT path; never enters src/common/
 *
 *    broker -> ctld    (2 RPCs):
 *        slurm_send_recv_controller_rc_msg
 *        - slurm-native frame; goes to the ctld at SLURM_CONF's
 *          SlurmctldHost:SlurmctldPort
 *        - depends on the slurmctld engineer's PR registering the 2
 *          msg_types (REQUEST_BROKER_UPDATE_REMOTE_STATE / TERMINAL_
 *          STATE) in src/common/. Until that lands, every call to the
 *          ctld_*_state wrapper here will fail with a non-fatal
 *          warning() and the broker continues serving.
 *
 *  Threading model
 *  ---------------
 *  Each wrapper is fully synchronous on the calling thread:
 *  state_machine tick / sync_ticker call the wrapper, the wrapper
 *  blocks for at most timeout_s and returns. There is no background
 *  egress worker pool; M08-T1 risks table accepts the trade-off given
 *  MVP throughput targets (<= 50 RPC/s).
\*****************************************************************************/

#include "config.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/xmalloc.h"

#include "broker_conf.h"
#include "broker_job.h"
#include "egress.h"
#include "persist.h"
#include "proto.h"
#include "stage.h"

/*****************************************************************************\
 *                       generic retry helper
 *
 * Exponential backoff. The argument-passing model uses a void* context
 * because it is the smallest portable closure for our two callbacks
 * (_send_recv_cb / _send_only_cb) without resorting to GCC nested
 * functions.
\*****************************************************************************/

typedef int (*_retry_fn_t)(void *arg);

static int _retry_n_times(_retry_fn_t fn, void *arg, int max_attempts,
			  int initial_backoff_ms)
{
	int rc = SLURM_ERROR;
	int wait_ms = initial_backoff_ms;

	for (int i = 0; i < max_attempts; i++) {
		rc = fn(arg);
		if (rc == SLURM_SUCCESS)
			return SLURM_SUCCESS;
		if (i + 1 == max_attempts)
			break;
		(void) usleep((useconds_t) wait_ms * 1000);
		wait_ms *= 2;
	}
	return rc;
}

/*****************************************************************************\
 *                       send-recv ctx (5 broker->broker RPCs)
\*****************************************************************************/

typedef struct {
	uint16_t  msg_type;
	void     *req;
	uint16_t  resp_type;
	void    **resp_out;       /* nullable */
	int       timeout_s;
} _peer_send_recv_ctx_t;

static int _peer_send_recv_cb(void *arg)
{
	_peer_send_recv_ctx_t *c = arg;
	void *resp = NULL;
	int rc;

	rc = proto_send_recv_to_peer(c->msg_type, c->req,
				     c->timeout_s, c->resp_type, &resp);
	if (rc != SLURM_SUCCESS)
		return rc;

	if (c->resp_out)
		*c->resp_out = resp;
	else if (resp)
		brokerd_free_msg_data(c->resp_type, resp);
	return SLURM_SUCCESS;
}

typedef struct {
	uint16_t msg_type;
	void    *req;
	int      timeout_s;
} _peer_send_only_ctx_t;

static int _peer_send_only_cb(void *arg)
{
	_peer_send_only_ctx_t *c = arg;

	return proto_send_to_peer(c->msg_type, c->req, c->timeout_s);
}

/*****************************************************************************\
 *                       lifecycle
\*****************************************************************************/

int egress_init(void)
{
	if (!g_broker_conf.remote_broker_host ||
	    !g_broker_conf.remote_broker_port) {
		error("egress_init: peer endpoint not configured");
		return SLURM_ERROR;
	}
	debug("egress_init: peer = %s:%u",
	      g_broker_conf.remote_broker_host,
	      g_broker_conf.remote_broker_port);
	return SLURM_SUCCESS;
}

void egress_fini(void)
{
	/* No state of our own. proto_fini() owns the peer endpoint. */
}

/*****************************************************************************\
 *                       broker -> broker
\*****************************************************************************/

int egress_forward_async(broker_job_t *job)
{
	brokerd_broker_forward_job_msg_t req = { 0 };
	brokerd_broker_ack_msg_t *resp = NULL;
	_peer_send_recv_ctx_t ctx;
	int rc;

	if (!job) {
		error("egress_forward_async: NULL job");
		return SLURM_ERROR;
	}

	/* Snapshot of the job-flat fields we need on the wire. The
	 * wrapper only borrows pointers; the job is the owner. */
	req.trace_id         = job->trace_id;
	req.hop_count        = job->hop_count;
	req.src_cluster      = job->src_cluster;
	req.src_job_id       = job->src_job_id;
	req.src_user_name    = job->src_user_name;
	req.remote_user_name = job->remote_user_name;
	req.target_partition = job->target_partition;
	req.app_name         = job->app_name;
	req.script_path      = job->script_path;

	ctx.msg_type  = BROKERD_REQUEST_BROKER_FORWARD_JOB;
	ctx.req       = &req;
	ctx.resp_type = BROKERD_RESPONSE_BROKER_ACK;
	ctx.resp_out  = (void **) &resp;
	ctx.timeout_s = 30;

	rc = _retry_n_times(_peer_send_recv_cb, &ctx, 3, 200);

	if (rc != SLURM_SUCCESS) {
		error("egress_forward: trace_id=%s send failed: %s",
		      job->trace_id, slurm_strerror(rc));
		broker_job_set_state(job, BROKER_STATE_FAILED,
				     "forward rpc failed");
		persist_async_request();
		return rc;
	}

	/* Receiver returned a typed ACK; check application-level error. */
	if (resp && resp->error_code != SLURM_SUCCESS) {
		int err = (int) resp->error_code;

		warning("egress_forward: trace_id=%s remote refused: %s",
		        job->trace_id, brokerd_strerror(err));
		brokerd_free_broker_ack_msg(resp);
		broker_job_set_state(job, BROKER_STATE_FAILED,
				     "forward refused by receiver");
		persist_async_request();
		return err;
	}

	/* Adopt the receiver-created dst_work_dir (canonical path on the
	 * remote side; the originator needs it for the upcoming rsync). */
	if (resp && resp->dst_work_dir && *resp->dst_work_dir) {
		slurm_mutex_lock(&job->lock);
		xfree(job->dst_work_dir);
		job->dst_work_dir = xstrdup(resp->dst_work_dir);
		slurm_mutex_unlock(&job->lock);
	}
	if (resp)
		brokerd_free_broker_ack_msg(resp);

	broker_job_set_state(job, BROKER_STATE_STAGING_IN, NULL);
	persist_async_request();

	/*
	 * Hand the job off to the rsync worker pool. The worker will
	 * eventually transition to STAGED_IN and call
	 * egress_staged_in_async() on success, or leave the job in
	 * STAGING_IN for the M09 timeout-and-retry watchdog on failure.
	 * Note: stage_submit_in() may itself transition the job to
	 * FAILED if the du -sb pre-flight exceeds max_stage_bytes.
	 */
	(void) stage_submit_in(job);

	info("egress_forward: trace_id=%s forwarded to peer, dst=%s, stage-in queued",
	     job->trace_id,
	     job->dst_work_dir ? job->dst_work_dir : "(none)");
	return SLURM_SUCCESS;
}

int egress_staged_in_async(broker_job_t *job)
{
	brokerd_broker_staged_in_msg_t req = { 0 };
	brokerd_broker_submitted_msg_t *resp = NULL;
	_peer_send_recv_ctx_t ctx;
	int rc;

	if (!job) {
		error("egress_staged_in_async: NULL job");
		return SLURM_ERROR;
	}

	req.trace_id  = job->trace_id;

	ctx.msg_type  = BROKERD_REQUEST_BROKER_STAGED_IN;
	ctx.req       = &req;
	ctx.resp_type = BROKERD_RESPONSE_BROKER_SUBMITTED;
	ctx.resp_out  = (void **) &resp;
	ctx.timeout_s = 60;  /* receiver may take a while inside sbatch */

	rc = _retry_n_times(_peer_send_recv_cb, &ctx, 3, 500);

	if (rc != SLURM_SUCCESS) {
		error("egress_staged_in: trace_id=%s send failed: %s",
		      job->trace_id, slurm_strerror(rc));
		broker_job_set_state(job, BROKER_STATE_FAILED,
				     "staged_in rpc failed");
		persist_async_request();
		return rc;
	}

	if (resp && resp->error_code != SLURM_SUCCESS) {
		int err = (int) resp->error_code;

		warning("egress_staged_in: trace_id=%s remote sbatch failed: %s",
		        job->trace_id, brokerd_strerror(err));
		brokerd_free_broker_submitted_msg(resp);
		broker_job_set_state(job, BROKER_STATE_FAILED,
				     "remote sbatch failed");
		persist_async_request();
		return err;
	}

	if (resp) {
		slurm_mutex_lock(&job->lock);
		job->remote_job_id = resp->remote_job_id;
		job->submit_time   = time(NULL);
		slurm_mutex_unlock(&job->lock);
		brokerd_free_broker_submitted_msg(resp);
	}

	broker_job_set_state(job, BROKER_STATE_SUBMITTED, NULL);
	persist_async_request();

	/* Fire one initial UPDATE_REMOTE_STATE so the ctld sees a
	 * remote_job_id immediately, instead of waiting for the next
	 * sync_ticker round. Failure here is non-fatal. */
	(void) ctld_update_remote_state(job);

	info("egress_staged_in: trace_id=%s -> remote_job_id=%u",
	     job->trace_id, job->remote_job_id);
	return SLURM_SUCCESS;
}

int egress_query_status_sync(char **trace_ids, uint32_t n,
			     brokerd_broker_status_msg_t **resp_out)
{
	brokerd_broker_query_status_msg_t req = { 0 };
	_peer_send_recv_ctx_t ctx;
	int rc;

	if (!resp_out) {
		error("egress_query_status_sync: NULL resp_out");
		return SLURM_ERROR;
	}
	*resp_out = NULL;

	if (!n || !trace_ids) {
		debug("egress_query_status_sync: empty query, no-op");
		return SLURM_SUCCESS;
	}

	req.trace_id_count = n;
	req.trace_ids      = trace_ids;

	ctx.msg_type  = BROKERD_REQUEST_BROKER_QUERY_STATUS;
	ctx.req       = &req;
	ctx.resp_type = BROKERD_RESPONSE_BROKER_STATUS;
	ctx.resp_out  = (void **) resp_out;
	ctx.timeout_s = 30;

	/* Two attempts only: sync_ticker runs every 10s, so a hard retry
	 * loop is wasteful; the next tick is a natural retry. */
	rc = _retry_n_times(_peer_send_recv_cb, &ctx, 2, 1000);
	if (rc != SLURM_SUCCESS) {
		warning("egress_query_status: %u trace_ids, send failed: %s",
		        n, slurm_strerror(rc));
	}
	return rc;
}

int egress_cancel_async(broker_job_t *job)
{
	brokerd_broker_cancel_msg_t req = { 0 };
	_peer_send_only_ctx_t ctx;
	int rc;

	if (!job) {
		error("egress_cancel_async: NULL job");
		return SLURM_ERROR;
	}

	/* Cross-broker cancel uses trace_id (the receiver looks it up in
	 * its local table). src_job_id stays zero on this path. */
	req.trace_id   = job->trace_id;
	req.src_job_id = 0;

	ctx.msg_type  = BROKERD_REQUEST_BROKER_CANCEL;
	ctx.req       = &req;
	ctx.timeout_s = 10;

	rc = _retry_n_times(_peer_send_only_cb, &ctx, 3, 200);
	if (rc != SLURM_SUCCESS) {
		warning("egress_cancel: trace_id=%s failed after retries: %s",
		        job->trace_id, slurm_strerror(rc));
		return rc;
	}

	slurm_mutex_lock(&job->lock);
	job->cancel_propagated = true;
	slurm_mutex_unlock(&job->lock);
	persist_async_request();

	info("egress_cancel: trace_id=%s cancel propagated to peer",
	     job->trace_id);
	return SLURM_SUCCESS;
}

int egress_cleanup_async(const char *trace_id)
{
	brokerd_broker_cleanup_msg_t req = { 0 };
	_peer_send_only_ctx_t ctx;
	int rc;

	if (!trace_id || !*trace_id) {
		error("egress_cleanup_async: NULL trace_id");
		return SLURM_ERROR;
	}

	req.trace_id  = (char *) trace_id;

	ctx.msg_type  = BROKERD_REQUEST_BROKER_CLEANUP;
	ctx.req       = &req;
	ctx.timeout_s = 10;

	rc = _retry_n_times(_peer_send_only_cb, &ctx, 3, 200);
	if (rc != SLURM_SUCCESS) {
		warning("egress_cleanup: trace_id=%s failed after retries: %s",
		        trace_id, slurm_strerror(rc));
		return rc;
	}

	info("egress_cleanup: trace_id=%s cleanup propagated to peer",
	     trace_id);
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       broker -> local ctld
 *
 * IMPORTANT: these go through slurm-native pack_msg() which only knows
 * about REQUEST_BROKER_UPDATE_REMOTE_STATE / REQUEST_BROKER_TERMINAL_
 * STATE once the slurmctld engineer's sister PR has registered the
 * matching numeric values + pack/unpack in src/common/. Until then,
 * slurm_send_recv_controller_rc_msg() will return SLURM_ERROR and the
 * wrapper logs a single warning. The broker stays alive and the next
 * sync_ticker cycle re-tries naturally.
 *
 * NEVER add a slurm_update_job(comment) anywhere. The whole point of
 * these two RPCs is to NOT pollute the user-visible comment field.
\*****************************************************************************/

int ctld_update_remote_state(broker_job_t *job)
{
	brokerd_broker_remote_state_msg_t req = { 0 };
	slurm_msg_t req_msg;
	int rc = SLURM_SUCCESS;

	if (!job) {
		error("ctld_update_remote_state: NULL job");
		return SLURM_ERROR;
	}

	req.src_job_id            = job->src_job_id;
	req.trace_id              = job->trace_id;
	req.remote_cluster_name   = job->dst_cluster;
	req.remote_partition_name = job->target_partition;
	req.remote_job_id         = job->remote_job_id;
	req.remote_state          = (uint32_t) job->state;
	req.remote_alloc_tres     = job->remote_alloc_tres;
	req.remote_start_time     = job->remote_start_time;

	slurm_msg_t_init(&req_msg);
	req_msg.msg_type         = BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE;
	req_msg.protocol_version = SLURM_PROTOCOL_VERSION;
	req_msg.data             = &req;

	if (slurm_send_recv_controller_rc_msg(&req_msg, &rc, NULL) < 0) {
		warning("ctld_update_remote_state: trace_id=%s send failed: %m",
		        job->trace_id);
		return SLURM_ERROR;
	}
	if (rc != SLURM_SUCCESS) {
		warning("ctld_update_remote_state: trace_id=%s ctld replied rc=%d (%s)",
		        job->trace_id, rc, slurm_strerror(rc));
	} else {
		debug2("ctld_update_remote_state: trace_id=%s state=%d remote_job_id=%u",
		       job->trace_id, (int) job->state,
		       job->remote_job_id);
	}
	return rc;
}

int ctld_inject_terminal_state(broker_job_t *job)
{
	brokerd_broker_terminal_state_msg_t req = { 0 };
	slurm_msg_t req_msg;
	int rc = SLURM_SUCCESS;

	if (!job) {
		error("ctld_inject_terminal_state: NULL job");
		return SLURM_ERROR;
	}

	req.base.src_job_id            = job->src_job_id;
	req.base.trace_id              = job->trace_id;
	req.base.remote_cluster_name   = job->dst_cluster;
	req.base.remote_partition_name = job->target_partition;
	req.base.remote_job_id         = job->remote_job_id;
	req.base.remote_state          = (uint32_t) job->state;
	req.base.remote_alloc_tres     = job->remote_alloc_tres;
	req.base.remote_start_time     = job->remote_start_time;
	req.remote_end_time            = job->remote_end_time;
	req.remote_exit_code           = job->remote_exit_code;

	slurm_msg_t_init(&req_msg);
	req_msg.msg_type         = BROKERD_REQUEST_BROKER_TERMINAL_STATE;
	req_msg.protocol_version = SLURM_PROTOCOL_VERSION;
	req_msg.data             = &req;

	if (slurm_send_recv_controller_rc_msg(&req_msg, &rc, NULL) < 0) {
		error("ctld_inject_terminal_state: trace_id=%s send failed: %m",
		      job->trace_id);
		return SLURM_ERROR;
	}
	if (rc != SLURM_SUCCESS) {
		error("ctld_inject_terminal_state: trace_id=%s ctld replied rc=%d (%s)",
		      job->trace_id, rc, slurm_strerror(rc));
		return rc;
	}
	info("ctld_inject_terminal_state: trace_id=%s state=%d exit=%d",
	     job->trace_id, (int) job->state, job->remote_exit_code);
	return SLURM_SUCCESS;
}
