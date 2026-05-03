/*****************************************************************************\
 *  handler_remote.c - dispatcher and stubs for broker -> broker inbound RPCs.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See handler_remote.h for
 *  the contract and doc/checklists/M05-listener.md §6.M05-T3.
 *
 *  M05 PR scope: dispatcher + stubs that build a sensible response
 *  frame (or no response, for fire-and-forget RPCs) and release the
 *  inbound payload. M07 PR replaces every stub body with real
 *  cross-cluster business logic.
\*****************************************************************************/

#include "config.h"

#include <inttypes.h>
#include <stdint.h>
#include <unistd.h>

#include "slurm/slurm.h"

#include "src/common/log.h"
#include "src/common/pack.h"
#include "src/common/slurm_protocol_socket.h"
#include "src/common/xmalloc.h"

#include "handler_remote.h"
#include "proto.h"

/*****************************************************************************\
 *                       reply helper
\*****************************************************************************/

/*
 * Send a single broker private wire frame (msg_type, payload) on
 * conn_fd. payload ownership is NOT consumed; caller is responsible for
 * freeing it. Returns SLURM_SUCCESS / SLURM_ERROR.
 */
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

/*****************************************************************************\
 *                       per-msg stubs
\*****************************************************************************/

void handle_broker_forward_job(void *payload, int conn_fd)
{
	brokerd_broker_ack_msg_t resp = {
		.error_code   = SLURM_SUCCESS,
		.trace_id     = NULL,
		.dst_work_dir = NULL,
	};
	brokerd_broker_forward_job_msg_t *m = payload;

	info("handler_remote: BROKER_FORWARD_JOB received, trace_id=%s "
	     "app=%s script=%s (M07 stub)",
	     m->trace_id ? m->trace_id : "(null)",
	     m->app_name ? m->app_name : "(null)",
	     m->script_path ? m->script_path : "(null)");

	/* TODO M07-T1: real implementation
	 *   - allocate broker_job_t with role=RECEIVER, fill app_name +
	 *     basename(script_path) + remote_user_name + target_partition
	 *     from m
	 *   - mkdir dst_work_dir under remote_user (sudo -u <remote_user>)
	 *   - the actual sbatch happens later (handle_broker_staged_in),
	 *     after the originator side rsync places the script file
	 *   - reply with ACK { error_code, trace_id, dst_work_dir } */
	resp.trace_id = m->trace_id; /* echo back for now */
	(void) _reply(conn_fd, BROKERD_RESPONSE_BROKER_ACK, &resp);

	brokerd_free_msg_data(BROKERD_REQUEST_BROKER_FORWARD_JOB, payload);
}

void handle_broker_staged_in(void *payload, int conn_fd)
{
	brokerd_broker_submitted_msg_t resp = {
		.error_code    = SLURM_SUCCESS,
		.trace_id      = NULL,
		.remote_job_id = 0,
	};
	brokerd_broker_staged_in_msg_t *m = payload;

	info("handler_remote: BROKER_STAGED_IN received, trace_id=%s (M07 stub)",
	     m->trace_id ? m->trace_id : "(null)");

	/* TODO M07-T2: look up local broker_job_t by trace_id; rewrite
	 * `source ...` lines in dst_work_dir/<basename(script_path)> using
	 * lookup_software.sh <remote_cluster> <app_name>; then run
	 *   sudo -u <remote_user> sbatch \
	 *        --partition=<target_partition> \
	 *        --chdir=<dst_work_dir> \
	 *        <dst_work_dir>/<basename(script_path)>
	 * Capture remote_job_id from sbatch stdout, reply with SUBMITTED.
	 * No version-bound job_desc_msg_t is involved: each side uses its
	 * own libslurm to drive sbatch. */
	resp.trace_id = m->trace_id;
	(void) _reply(conn_fd, BROKERD_RESPONSE_BROKER_SUBMITTED, &resp);

	brokerd_free_msg_data(BROKERD_REQUEST_BROKER_STAGED_IN, payload);
}

void handle_broker_query_status(void *payload, int conn_fd)
{
	brokerd_broker_status_msg_t resp = {
		.entry_count = 0,
		.entries     = NULL,
	};
	brokerd_broker_query_status_msg_t *m = payload;

	info("handler_remote: BROKER_QUERY_STATUS received, count=%u (M07 stub)",
	     m->trace_id_count);

	/* TODO M07-T3: for each trace_id, look up local broker_job and
	 * call slurm_load_job(remote_job_id) to fetch state, build
	 * brokerd_broker_status_entry_t array. */
	(void) _reply(conn_fd, BROKERD_RESPONSE_BROKER_STATUS, &resp);

	brokerd_free_msg_data(BROKERD_REQUEST_BROKER_QUERY_STATUS, payload);
}

void handle_broker_cancel(void *payload, int conn_fd)
{
	brokerd_broker_cancel_msg_t *m = payload;

	(void) conn_fd;  /* fire-and-forget: no reply on this RPC */
	info("handler_remote: BROKER_CANCEL received, trace_id=%s src_job_id=%u (M07 stub)",
	     m->trace_id ? m->trace_id : "(null)", m->src_job_id);

	/* TODO M07-T4: slurm_kill_job(remote_job_id) under remote_user,
	 * mark broker_job CANCELLED, persist_async_request(). */
	brokerd_free_msg_data(BROKERD_REQUEST_BROKER_CANCEL, payload);
}

void handle_broker_cleanup(void *payload, int conn_fd)
{
	brokerd_broker_cleanup_msg_t *m = payload;

	(void) conn_fd;  /* fire-and-forget */
	info("handler_remote: BROKER_CLEANUP received, trace_id=%s (M07 stub)",
	     m->trace_id ? m->trace_id : "(null)");

	/* TODO M07-T5: schedule deletion of remote dst_work_dir at
	 * RemoteWorkDirRetentionHours (or ...FailureRetentionDays for
	 * failed jobs). */
	brokerd_free_msg_data(BROKERD_REQUEST_BROKER_CLEANUP, payload);
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
		handle_broker_forward_job(payload, conn_fd);
		break;
	case BROKERD_REQUEST_BROKER_STAGED_IN:
		handle_broker_staged_in(payload, conn_fd);
		break;
	case BROKERD_REQUEST_BROKER_QUERY_STATUS:
		handle_broker_query_status(payload, conn_fd);
		break;
	case BROKERD_REQUEST_BROKER_CANCEL:
		handle_broker_cancel(payload, conn_fd);
		break;
	case BROKERD_REQUEST_BROKER_CLEANUP:
		handle_broker_cleanup(payload, conn_fd);
		break;
	default:
		error("dispatch_remote_msg: unsupported msg_type=%s from %s",
		      brokerd_msg_type_str(msg_type), addr_str);
		brokerd_free_msg_data(msg_type, payload);
		break;
	}
}
