/*****************************************************************************\
 *  handler_ctld.c - dispatcher and stubs for ctld -> broker inbound RPCs.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See handler_ctld.h for the
 *  contract and doc/checklists/M05-listener.md §6.M05-T3 for the dispatch
 *  table design.
 *
 *  M05 PR scope: dispatcher + per-msg stubs that just acknowledge with
 *  SLURM_SUCCESS so end-to-end smoke tests (when the slurmctld PR lands)
 *  do not fail open. M06 PR replaces every stub body with real business
 *  logic that drives the broker_job table and egress workers.
\*****************************************************************************/

#include "config.h"

#include <inttypes.h>
#include <stdint.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/log.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_defs.h"

#include "handler_ctld.h"
#include "proto.h"

/*
 * The broker uses BROKERD_REQUEST_FORWARD_JOB / BROKERD_REQUEST_BROKER_CANCEL
 * macros internally (proto.h). The slurmctld PR will register the same
 * numeric values under unprefixed names REQUEST_FORWARD_JOB /
 * REQUEST_BROKER_CANCEL inside src/common/slurm_protocol_defs.h.
 *
 * Until that lands, those unprefixed macros are not defined here, so we
 * dispatch on the broker's own BROKERD_* names; once both sides ship the
 * same uint16_t value, slurm_receive_msg() will set msg->msg_type to the
 * matching numeric value and our switch will catch it correctly.
 */

void handle_forward_job(slurm_msg_t *msg)
{
	info("handler_ctld: REQUEST_FORWARD_JOB received from uid=%u (M06 stub)",
	     msg->auth_uid);
	/* TODO M06-T1: real implementation
	 *   - extract forward_job_msg_t fields from msg->data
	 *   - allocate broker_job_t, fill identity / cluster / work_dir
	 *   - generate trace_id, broker_job_table_add(job)
	 *   - persist_async_request()
	 *   - reply with brokerd_forward_job_resp_msg_t { error_code, trace_id }
	 */
	slurm_send_rc_msg(msg, SLURM_SUCCESS);
}

void handle_cancel_from_ctld(slurm_msg_t *msg)
{
	info("handler_ctld: REQUEST_BROKER_CANCEL (from ctld) received from uid=%u (M06 stub)",
	     msg->auth_uid);
	/* TODO M06-T2: real implementation
	 *   - look up broker_job_t by msg payload's src_job_id
	 *   - mark job->cancel_requested = true
	 *   - state machine tick will propagate via egress_cancel_async()
	 */
	slurm_send_rc_msg(msg, SLURM_SUCCESS);
}

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
		handle_forward_job(msg);
		break;
	case BROKERD_REQUEST_BROKER_CANCEL:
		handle_cancel_from_ctld(msg);
		break;
	default:
		error("dispatch_ctld_msg: unsupported msg_type=%u from %s",
		      msg->msg_type, addr_str);
		slurm_send_rc_msg(msg, SLURM_PROTOCOL_INVALID_MESSAGE);
		break;
	}
}
