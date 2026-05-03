/*****************************************************************************\
 *  proto_pack.c - pack/unpack for the 11 broker RPC payloads.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See proto.h for the
 *  contract and doc/checklists/M04-rpc.md §2.3 / §6.x for field order.
 *
 *  Wire-format strategy
 *  --------------------
 *  Slurm's _pack_job_desc_msg() / _unpack_job_desc_msg() are file-static,
 *  but the public pack_msg()/unpack_msg() route REQUEST_SUBMIT_BATCH_JOB
 *  to exactly that pair. We therefore wrap a job_desc_msg_t inside a
 *  REQUEST_SUBMIT_BATCH_JOB slurm_msg_t, pack it into an inner buf_t,
 *  and embed the bytes into the outer broker frame using packmem(). The
 *  inverse path uses unpackmem_xmalloc + create_buf + unpack_msg.
 *
 *  Every field is written in the order documented in proto.h. Adding new
 *  fields MUST be append-only (after a protocol version bump if the new
 *  fields are mandatory).
\*****************************************************************************/

#include "config.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "slurm/slurm.h"

#include "src/common/log.h"
#include "src/common/pack.h"
#include "src/common/slurm_protocol_common.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/slurm_protocol_pack.h"
#include "src/common/xmalloc.h"

#include "proto.h"

/*****************************************************************************\
 *                       embedded job_desc helpers
 *
 * LEGACY_M04_TRANSITIONAL: only used by the 4 ctld<->broker payloads
 * (REQUEST_FORWARD_JOB / RESPONSE_FORWARD_JOB / REMOTE_STATE / TERMINAL_
 * STATE) which are themselves marked legacy. ctld<->broker traffic is
 * always same-host same-libslurm, so the slurm-version coupling carried
 * by these helpers is acceptable in that context only.
 *
 * The cross-cluster broker<->broker payloads (BROKER_FORWARD_JOB et al)
 * deliberately do NOT call these helpers; that path is fully
 * slurm-version-INDEPENDENT.
\*****************************************************************************/

/* Pack a job_desc_msg_t into the outer frame as a length-prefixed blob.
 * NULL is encoded as a zero-length blob, which the unpack mirror decodes
 * back to NULL. */
static void _pack_job_desc(job_desc_msg_t *jd, buf_t *outer, uint16_t pv)
{
	slurm_msg_t msg;
	buf_t *inner;

	if (!jd) {
		packmem(NULL, 0, outer);
		return;
	}

	slurm_msg_t_init(&msg);
	msg.msg_type = REQUEST_SUBMIT_BATCH_JOB;
	msg.protocol_version = pv;
	msg.data = jd;

	inner = init_buf(BUF_SIZE);
	if (pack_msg(&msg, inner)) {
		error("%s: pack_msg failed", __func__);
		packmem(NULL, 0, outer);
		FREE_NULL_BUFFER(inner);
		return;
	}
	packmem(get_buf_data(inner), get_buf_offset(inner), outer);
	FREE_NULL_BUFFER(inner);
}

static int _unpack_job_desc(job_desc_msg_t **out, buf_t *outer, uint16_t pv)
{
	slurm_msg_t msg;
	buf_t *inner = NULL;
	char *raw = NULL;
	uint32_t raw_len = 0;

	*out = NULL;

	if (unpackmem_xmalloc(&raw, &raw_len, outer))
		return SLURM_ERROR;
	if (raw_len == 0) {
		xfree(raw);
		return SLURM_SUCCESS;
	}

	/* create_buf takes ownership of raw */
	inner = create_buf(raw, raw_len);
	if (!inner) {
		xfree(raw);
		return SLURM_ERROR;
	}

	slurm_msg_t_init(&msg);
	msg.msg_type = REQUEST_SUBMIT_BATCH_JOB;
	msg.protocol_version = pv;
	msg.data = NULL;

	if (unpack_msg(&msg, inner)) {
		error("%s: unpack_msg failed", __func__);
		FREE_NULL_BUFFER(inner);
		return SLURM_ERROR;
	}
	*out = msg.data;
	FREE_NULL_BUFFER(inner);
	return SLURM_SUCCESS;
}

/* nested status entry */
static void _pack_status_entry(brokerd_broker_status_entry_t *e,
			       buf_t *buffer, uint16_t pv)
{
	(void) pv;
	packstr(e->trace_id, buffer);
	pack32(e->remote_state, buffer);
	pack_time(e->remote_start_time, buffer);
	pack_time(e->remote_end_time, buffer);
	packstr(e->remote_alloc_tres, buffer);
	pack32((uint32_t) e->remote_exit_code, buffer);
}

static int _unpack_status_entry(brokerd_broker_status_entry_t *e,
				buf_t *buffer, uint16_t pv)
{
	uint32_t u32;

	(void) pv;
	memset(e, 0, sizeof(*e));
	safe_unpackstr(&e->trace_id, buffer);
	safe_unpack32(&e->remote_state, buffer);
	safe_unpack_time(&e->remote_start_time, buffer);
	safe_unpack_time(&e->remote_end_time, buffer);
	safe_unpackstr(&e->remote_alloc_tres, buffer);
	safe_unpack32(&u32, buffer);
	e->remote_exit_code = (int32_t) u32;
	return SLURM_SUCCESS;

unpack_error:
	xfree(e->trace_id);
	xfree(e->remote_alloc_tres);
	memset(e, 0, sizeof(*e));
	return SLURM_ERROR;
}

/*****************************************************************************\
 *                       per-msg pack / unpack
 *
 * The 4 ctld<->broker pairs (REQUEST_FORWARD_JOB, RESPONSE_FORWARD_JOB,
 * REQUEST_BROKER_UPDATE_REMOTE_STATE, REQUEST_BROKER_TERMINAL_STATE)
 * are LEGACY_M04_TRANSITIONAL: each one will be removed in the M05
 * listener PR once the slurmctld engineer registers the matching
 * pack/unpack inside src/common/slurm_protocol_pack.c. The other 7
 * broker<->broker pairs are PERMANENT.
\*****************************************************************************/

/* ===== LEGACY_M04_TRANSITIONAL: REQUEST_FORWARD_JOB ===== */

static void _pack_forward_job_msg(brokerd_forward_job_msg_t *m,
				  buf_t *buffer, uint16_t pv)
{
	pack32(m->src_job_id, buffer);
	pack32(m->src_uid, buffer);
	pack32(m->src_gid, buffer);
	packstr(m->src_user_name, buffer);
	packstr(m->target_cluster, buffer);
	packstr(m->src_work_dir, buffer);
	packstr(m->script_path, buffer);
	packstr(m->account, buffer);
	packstr(m->app_name, buffer);
	_pack_job_desc(m->job_desc, buffer, pv);
}

static int _unpack_forward_job_msg(brokerd_forward_job_msg_t **out,
				   buf_t *buffer, uint16_t pv)
{
	brokerd_forward_job_msg_t *m = xmalloc(sizeof(*m));

	safe_unpack32(&m->src_job_id, buffer);
	safe_unpack32(&m->src_uid, buffer);
	safe_unpack32(&m->src_gid, buffer);
	safe_unpackstr(&m->src_user_name, buffer);
	safe_unpackstr(&m->target_cluster, buffer);
	safe_unpackstr(&m->src_work_dir, buffer);
	safe_unpackstr(&m->script_path, buffer);
	safe_unpackstr(&m->account, buffer);
	safe_unpackstr(&m->app_name, buffer);
	if (_unpack_job_desc(&m->job_desc, buffer, pv))
		goto unpack_error;
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_forward_job_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/* ===== LEGACY_M04_TRANSITIONAL: RESPONSE_FORWARD_JOB ===== */

static void _pack_forward_job_resp_msg(brokerd_forward_job_resp_msg_t *m,
				       buf_t *buffer, uint16_t pv)
{
	(void) pv;
	pack32(m->error_code, buffer);
	packstr(m->trace_id, buffer);
}

static int _unpack_forward_job_resp_msg(brokerd_forward_job_resp_msg_t **out,
					buf_t *buffer, uint16_t pv)
{
	brokerd_forward_job_resp_msg_t *m = xmalloc(sizeof(*m));

	(void) pv;
	safe_unpack32(&m->error_code, buffer);
	safe_unpackstr(&m->trace_id, buffer);
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_forward_job_resp_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/* ===== REQUEST_BROKER_FORWARD_JOB =====
 *
 * Slurm-version-INDEPENDENT wire layout: every field is a flat string
 * or fixed-width integer; no embedded job_desc_msg_t. This is what
 * lets a 24.05 broker safely forward to a 23.11 broker.
 */

static void _pack_broker_forward_job_msg(brokerd_broker_forward_job_msg_t *m,
					 buf_t *buffer, uint16_t pv)
{
	(void) pv;
	packstr(m->trace_id, buffer);
	pack8(m->hop_count, buffer);
	packstr(m->src_cluster, buffer);
	pack32(m->src_job_id, buffer);
	packstr(m->src_user_name, buffer);
	packstr(m->remote_user_name, buffer);
	packstr(m->target_partition, buffer);
	packstr(m->app_name, buffer);
	packstr(m->script_path, buffer);
}

static int _unpack_broker_forward_job_msg(
	brokerd_broker_forward_job_msg_t **out, buf_t *buffer, uint16_t pv)
{
	brokerd_broker_forward_job_msg_t *m = xmalloc(sizeof(*m));

	(void) pv;
	safe_unpackstr(&m->trace_id, buffer);
	safe_unpack8(&m->hop_count, buffer);
	safe_unpackstr(&m->src_cluster, buffer);
	safe_unpack32(&m->src_job_id, buffer);
	safe_unpackstr(&m->src_user_name, buffer);
	safe_unpackstr(&m->remote_user_name, buffer);
	safe_unpackstr(&m->target_partition, buffer);
	safe_unpackstr(&m->app_name, buffer);
	safe_unpackstr(&m->script_path, buffer);
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_broker_forward_job_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/* ===== RESPONSE_BROKER_ACK ===== */

static void _pack_broker_ack_msg(brokerd_broker_ack_msg_t *m,
				 buf_t *buffer, uint16_t pv)
{
	(void) pv;
	pack32(m->error_code, buffer);
	packstr(m->trace_id, buffer);
	packstr(m->dst_work_dir, buffer);
}

static int _unpack_broker_ack_msg(brokerd_broker_ack_msg_t **out,
				  buf_t *buffer, uint16_t pv)
{
	brokerd_broker_ack_msg_t *m = xmalloc(sizeof(*m));

	(void) pv;
	safe_unpack32(&m->error_code, buffer);
	safe_unpackstr(&m->trace_id, buffer);
	safe_unpackstr(&m->dst_work_dir, buffer);
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_broker_ack_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/* ===== REQUEST_BROKER_STAGED_IN ===== */

static void _pack_broker_staged_in_msg(brokerd_broker_staged_in_msg_t *m,
				       buf_t *buffer, uint16_t pv)
{
	(void) pv;
	packstr(m->trace_id, buffer);
}

static int _unpack_broker_staged_in_msg(brokerd_broker_staged_in_msg_t **out,
					buf_t *buffer, uint16_t pv)
{
	brokerd_broker_staged_in_msg_t *m = xmalloc(sizeof(*m));

	(void) pv;
	safe_unpackstr(&m->trace_id, buffer);
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_broker_staged_in_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/* ===== RESPONSE_BROKER_SUBMITTED ===== */

static void _pack_broker_submitted_msg(brokerd_broker_submitted_msg_t *m,
				       buf_t *buffer, uint16_t pv)
{
	(void) pv;
	pack32(m->error_code, buffer);
	packstr(m->trace_id, buffer);
	pack32(m->remote_job_id, buffer);
}

static int _unpack_broker_submitted_msg(brokerd_broker_submitted_msg_t **out,
					buf_t *buffer, uint16_t pv)
{
	brokerd_broker_submitted_msg_t *m = xmalloc(sizeof(*m));

	(void) pv;
	safe_unpack32(&m->error_code, buffer);
	safe_unpackstr(&m->trace_id, buffer);
	safe_unpack32(&m->remote_job_id, buffer);
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_broker_submitted_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/* ===== REQUEST_BROKER_QUERY_STATUS ===== */

static void _pack_broker_query_status_msg(brokerd_broker_query_status_msg_t *m,
					  buf_t *buffer, uint16_t pv)
{
	(void) pv;
	pack32(m->trace_id_count, buffer);
	for (uint32_t i = 0; i < m->trace_id_count; i++)
		packstr(m->trace_ids ? m->trace_ids[i] : NULL, buffer);
}

static int _unpack_broker_query_status_msg(
	brokerd_broker_query_status_msg_t **out, buf_t *buffer, uint16_t pv)
{
	brokerd_broker_query_status_msg_t *m = xmalloc(sizeof(*m));

	(void) pv;
	safe_unpack32(&m->trace_id_count, buffer);
	if (m->trace_id_count) {
		m->trace_ids = xcalloc(m->trace_id_count, sizeof(char *));
		for (uint32_t i = 0; i < m->trace_id_count; i++)
			safe_unpackstr(&m->trace_ids[i], buffer);
	}
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_broker_query_status_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/* ===== RESPONSE_BROKER_STATUS ===== */

static void _pack_broker_status_msg(brokerd_broker_status_msg_t *m,
				    buf_t *buffer, uint16_t pv)
{
	pack32(m->entry_count, buffer);
	for (uint32_t i = 0; i < m->entry_count; i++)
		_pack_status_entry(&m->entries[i], buffer, pv);
}

static int _unpack_broker_status_msg(brokerd_broker_status_msg_t **out,
				     buf_t *buffer, uint16_t pv)
{
	brokerd_broker_status_msg_t *m = xmalloc(sizeof(*m));

	safe_unpack32(&m->entry_count, buffer);
	if (m->entry_count) {
		m->entries = xcalloc(m->entry_count,
				     sizeof(brokerd_broker_status_entry_t));
		for (uint32_t i = 0; i < m->entry_count; i++) {
			if (_unpack_status_entry(&m->entries[i], buffer, pv))
				goto unpack_error;
		}
	}
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_broker_status_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/* ===== REQUEST_BROKER_CANCEL ===== */

static void _pack_broker_cancel_msg(brokerd_broker_cancel_msg_t *m,
				    buf_t *buffer, uint16_t pv)
{
	(void) pv;
	pack32(m->src_job_id, buffer);
	packstr(m->trace_id, buffer);
}

static int _unpack_broker_cancel_msg(brokerd_broker_cancel_msg_t **out,
				     buf_t *buffer, uint16_t pv)
{
	brokerd_broker_cancel_msg_t *m = xmalloc(sizeof(*m));

	(void) pv;
	safe_unpack32(&m->src_job_id, buffer);
	safe_unpackstr(&m->trace_id, buffer);
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_broker_cancel_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/* ===== REQUEST_BROKER_CLEANUP ===== */

static void _pack_broker_cleanup_msg(brokerd_broker_cleanup_msg_t *m,
				     buf_t *buffer, uint16_t pv)
{
	(void) pv;
	packstr(m->trace_id, buffer);
}

static int _unpack_broker_cleanup_msg(brokerd_broker_cleanup_msg_t **out,
				      buf_t *buffer, uint16_t pv)
{
	brokerd_broker_cleanup_msg_t *m = xmalloc(sizeof(*m));

	(void) pv;
	safe_unpackstr(&m->trace_id, buffer);
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_broker_cleanup_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/* ===== LEGACY_M04_TRANSITIONAL: REQUEST_BROKER_UPDATE_REMOTE_STATE ===== */

static void _pack_broker_remote_state_msg(brokerd_broker_remote_state_msg_t *m,
					  buf_t *buffer, uint16_t pv)
{
	(void) pv;
	pack32(m->src_job_id, buffer);
	packstr(m->trace_id, buffer);
	packstr(m->remote_cluster_name, buffer);
	packstr(m->remote_partition_name, buffer);
	pack32(m->remote_job_id, buffer);
	pack32(m->remote_state, buffer);
	packstr(m->remote_alloc_tres, buffer);
	pack_time(m->remote_start_time, buffer);
}

static int _unpack_broker_remote_state_msg(
	brokerd_broker_remote_state_msg_t **out, buf_t *buffer, uint16_t pv)
{
	brokerd_broker_remote_state_msg_t *m = xmalloc(sizeof(*m));

	(void) pv;
	safe_unpack32(&m->src_job_id, buffer);
	safe_unpackstr(&m->trace_id, buffer);
	safe_unpackstr(&m->remote_cluster_name, buffer);
	safe_unpackstr(&m->remote_partition_name, buffer);
	safe_unpack32(&m->remote_job_id, buffer);
	safe_unpack32(&m->remote_state, buffer);
	safe_unpackstr(&m->remote_alloc_tres, buffer);
	safe_unpack_time(&m->remote_start_time, buffer);
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_broker_remote_state_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/* ===== LEGACY_M04_TRANSITIONAL: REQUEST_BROKER_TERMINAL_STATE =====
 *
 * Wire layout = the 8 fields of remote_state, then the 2 terminal-only
 * fields. We hand-write the body so the layout stays explicit (and so
 * future field additions can land on either struct without reordering
 * the wire). */

static void _pack_broker_terminal_state_msg(
	brokerd_broker_terminal_state_msg_t *m, buf_t *buffer, uint16_t pv)
{
	(void) pv;
	pack32(m->base.src_job_id, buffer);
	packstr(m->base.trace_id, buffer);
	packstr(m->base.remote_cluster_name, buffer);
	packstr(m->base.remote_partition_name, buffer);
	pack32(m->base.remote_job_id, buffer);
	pack32(m->base.remote_state, buffer);
	packstr(m->base.remote_alloc_tres, buffer);
	pack_time(m->base.remote_start_time, buffer);
	pack_time(m->remote_end_time, buffer);
	pack32((uint32_t) m->remote_exit_code, buffer);
}

static int _unpack_broker_terminal_state_msg(
	brokerd_broker_terminal_state_msg_t **out, buf_t *buffer, uint16_t pv)
{
	brokerd_broker_terminal_state_msg_t *m = xmalloc(sizeof(*m));
	uint32_t u32;

	(void) pv;
	safe_unpack32(&m->base.src_job_id, buffer);
	safe_unpackstr(&m->base.trace_id, buffer);
	safe_unpackstr(&m->base.remote_cluster_name, buffer);
	safe_unpackstr(&m->base.remote_partition_name, buffer);
	safe_unpack32(&m->base.remote_job_id, buffer);
	safe_unpack32(&m->base.remote_state, buffer);
	safe_unpackstr(&m->base.remote_alloc_tres, buffer);
	safe_unpack_time(&m->base.remote_start_time, buffer);
	safe_unpack_time(&m->remote_end_time, buffer);
	safe_unpack32(&u32, buffer);
	m->remote_exit_code = (int32_t) u32;
	*out = m;
	return SLURM_SUCCESS;

unpack_error:
	brokerd_free_broker_terminal_state_msg(m);
	*out = NULL;
	return SLURM_ERROR;
}

/*****************************************************************************\
 *                       dispatcher
\*****************************************************************************/

int brokerd_pack_msg(uint16_t msg_type, void *payload,
		     uint16_t protocol_version, buf_t *buffer)
{
	if (!buffer) {
		error("%s: NULL buffer", __func__);
		return SLURM_ERROR;
	}
	if (protocol_version < SLURM_MIN_PROTOCOL_VERSION) {
		error("%s: unsupported protocol_version %hu",
		      __func__, protocol_version);
		return SLURM_ERROR;
	}
	if (!payload) {
		error("%s: NULL payload for msg_type=%s",
		      __func__, brokerd_msg_type_str(msg_type));
		return SLURM_ERROR;
	}

	switch (msg_type) {
	/* LEGACY_M04_TRANSITIONAL: 4 ctld<->broker cases (M05 PR removes). */
	case BROKERD_REQUEST_FORWARD_JOB:
		_pack_forward_job_msg(payload, buffer, protocol_version);
		break;
	case BROKERD_RESPONSE_FORWARD_JOB:
		_pack_forward_job_resp_msg(payload, buffer, protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE:
		_pack_broker_remote_state_msg(payload, buffer,
					      protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_TERMINAL_STATE:
		_pack_broker_terminal_state_msg(payload, buffer,
						protocol_version);
		break;
	/* PERMANENT: 8 broker<->broker cases. */
	case BROKERD_REQUEST_BROKER_FORWARD_JOB:
		_pack_broker_forward_job_msg(payload, buffer,
					     protocol_version);
		break;
	case BROKERD_RESPONSE_BROKER_ACK:
		_pack_broker_ack_msg(payload, buffer, protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_STAGED_IN:
		_pack_broker_staged_in_msg(payload, buffer, protocol_version);
		break;
	case BROKERD_RESPONSE_BROKER_SUBMITTED:
		_pack_broker_submitted_msg(payload, buffer, protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_QUERY_STATUS:
		_pack_broker_query_status_msg(payload, buffer,
					      protocol_version);
		break;
	case BROKERD_RESPONSE_BROKER_STATUS:
		_pack_broker_status_msg(payload, buffer, protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_CANCEL:
		_pack_broker_cancel_msg(payload, buffer, protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_CLEANUP:
		_pack_broker_cleanup_msg(payload, buffer, protocol_version);
		break;
	default:
		error("%s: unknown msg_type %hu", __func__, msg_type);
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

int brokerd_unpack_msg(uint16_t msg_type, void **payload_out,
		       uint16_t protocol_version, buf_t *buffer)
{
	int rc = SLURM_ERROR;

	if (!buffer || !payload_out) {
		error("%s: NULL arg", __func__);
		return SLURM_ERROR;
	}
	if (protocol_version < SLURM_MIN_PROTOCOL_VERSION) {
		error("%s: unsupported protocol_version %hu",
		      __func__, protocol_version);
		return SLURM_ERROR;
	}

	*payload_out = NULL;

	switch (msg_type) {
	/* LEGACY_M04_TRANSITIONAL: 4 ctld<->broker cases (M05 PR removes). */
	case BROKERD_REQUEST_FORWARD_JOB:
		rc = _unpack_forward_job_msg(
			(brokerd_forward_job_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	case BROKERD_RESPONSE_FORWARD_JOB:
		rc = _unpack_forward_job_resp_msg(
			(brokerd_forward_job_resp_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE:
		rc = _unpack_broker_remote_state_msg(
			(brokerd_broker_remote_state_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_TERMINAL_STATE:
		rc = _unpack_broker_terminal_state_msg(
			(brokerd_broker_terminal_state_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	/* PERMANENT: 8 broker<->broker cases. */
	case BROKERD_REQUEST_BROKER_FORWARD_JOB:
		rc = _unpack_broker_forward_job_msg(
			(brokerd_broker_forward_job_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	case BROKERD_RESPONSE_BROKER_ACK:
		rc = _unpack_broker_ack_msg(
			(brokerd_broker_ack_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_STAGED_IN:
		rc = _unpack_broker_staged_in_msg(
			(brokerd_broker_staged_in_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	case BROKERD_RESPONSE_BROKER_SUBMITTED:
		rc = _unpack_broker_submitted_msg(
			(brokerd_broker_submitted_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_QUERY_STATUS:
		rc = _unpack_broker_query_status_msg(
			(brokerd_broker_query_status_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	case BROKERD_RESPONSE_BROKER_STATUS:
		rc = _unpack_broker_status_msg(
			(brokerd_broker_status_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_CANCEL:
		rc = _unpack_broker_cancel_msg(
			(brokerd_broker_cancel_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	case BROKERD_REQUEST_BROKER_CLEANUP:
		rc = _unpack_broker_cleanup_msg(
			(brokerd_broker_cleanup_msg_t **) payload_out,
			buffer, protocol_version);
		break;
	default:
		error("%s: unknown msg_type %hu", __func__, msg_type);
		rc = SLURM_ERROR;
		break;
	}
	return rc;
}

const char *brokerd_msg_type_str(uint16_t msg_type)
{
	switch (msg_type) {
	case BROKERD_REQUEST_FORWARD_JOB:
		return "REQUEST_FORWARD_JOB";
	case BROKERD_RESPONSE_FORWARD_JOB:
		return "RESPONSE_FORWARD_JOB";
	case BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE:
		return "REQUEST_BROKER_UPDATE_REMOTE_STATE";
	case BROKERD_REQUEST_BROKER_TERMINAL_STATE:
		return "REQUEST_BROKER_TERMINAL_STATE";
	case BROKERD_REQUEST_BROKER_FORWARD_JOB:
		return "REQUEST_BROKER_FORWARD_JOB";
	case BROKERD_RESPONSE_BROKER_ACK:
		return "RESPONSE_BROKER_ACK";
	case BROKERD_REQUEST_BROKER_STAGED_IN:
		return "REQUEST_BROKER_STAGED_IN";
	case BROKERD_RESPONSE_BROKER_SUBMITTED:
		return "RESPONSE_BROKER_SUBMITTED";
	case BROKERD_REQUEST_BROKER_QUERY_STATUS:
		return "REQUEST_BROKER_QUERY_STATUS";
	case BROKERD_RESPONSE_BROKER_STATUS:
		return "RESPONSE_BROKER_STATUS";
	case BROKERD_REQUEST_BROKER_CANCEL:
		return "REQUEST_BROKER_CANCEL";
	case BROKERD_REQUEST_BROKER_CLEANUP:
		return "REQUEST_BROKER_CLEANUP";
	default:
		return "UNKNOWN_BROKER_MSG";
	}
}
