/*****************************************************************************\
 *  proto.c - lifecycle, free helpers, strerror, and synchronous send_recv
 *            wrapper for the slurmbrokerd RPC layer.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See proto.h for the full
 *  contract and doc/checklists/M04-rpc.md §2 / §6 for the wire-level
 *  specification.
 *
 *  Wire frame (one round-trip = one frame request, one frame response)
 *  ------------------------------------------------------------------
 *      uint32  magic        = BROKERD_WIRE_MAGIC ('BRKR')
 *      uint16  proto_ver    = SLURM_PROTOCOL_VERSION (caller side)
 *      uint16  msg_type     = BROKERD_REQUEST_*  /  BROKERD_RESPONSE_*
 *      <auth_blob>          = auth_g_pack(cred, ...) inline
 *      <payload_blob>       = brokerd_pack_msg(msg_type, payload, ...)
 *
 *  The whole frame is then handed to slurm_msg_sendto() which prepends
 *  a 4-byte network-order length prefix; slurm_msg_recvfrom_timeout()
 *  reads it back symmetrically. Any frame whose magic does not match
 *  BROKERD_WIRE_MAGIC is dropped without further decoding.
 *
 *  This module reuses the Slurm-native auth plugin (loaded by slurm_init
 *  in broker_init), but it deliberately does not use the slurm_msg_t /
 *  pack_msg() dispatcher, which would require teaching slurm-native code
 *  about the broker's msg_type IDs. Per the workspace rule, all M04
 *  logic lives under src/slurmbrokerd/.
\*****************************************************************************/

#include "config.h"

#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/pack.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_common.h"
#include "src/common/slurm_protocol_socket.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/interfaces/auth.h"

#include "broker_conf.h"
#include "proto.h"

/*****************************************************************************\
 *                       module-local state
\*****************************************************************************/

static bool          proto_inited;
static slurm_addr_t  g_peer_addr;
static char         *g_peer_host;
static uint16_t      g_peer_port;

/* Default request/response timeout (ms) when caller passes timeout_s <= 0.
 * 30s matches the 30s checkpoint cadence and stage_worker scheduling tick
 * in the design doc; it is an upper bound, not a typical case. */
#define BROKERD_DEFAULT_TIMEOUT_MS  30000

/*****************************************************************************\
 *                       free helpers
 *
 * The 4 ctld<->broker free helpers (forward_job, forward_job_resp,
 * remote_state, terminal_state) are LEGACY_M04_TRANSITIONAL: the M05
 * listener PR removes them once the slurmctld engineer registers the
 * native pack_msg/unpack_msg path in src/common/. The 7 broker<->broker
 * helpers + the dual-purpose 8016 cancel helper are PERMANENT.
\*****************************************************************************/

/* LEGACY_M04_TRANSITIONAL */
void brokerd_free_forward_job_msg(brokerd_forward_job_msg_t *m)
{
	if (!m)
		return;
	xfree(m->src_user_name);
	xfree(m->target_cluster);
	xfree(m->src_work_dir);
	xfree(m->script_path);
	xfree(m->account);
	xfree(m->app_name);
	if (m->job_desc)
		slurm_free_job_desc_msg(m->job_desc);
	xfree(m);
}

/* LEGACY_M04_TRANSITIONAL */
void brokerd_free_forward_job_resp_msg(brokerd_forward_job_resp_msg_t *m)
{
	if (!m)
		return;
	xfree(m->trace_id);
	xfree(m);
}

void brokerd_free_broker_forward_job_msg(brokerd_broker_forward_job_msg_t *m)
{
	if (!m)
		return;
	xfree(m->trace_id);
	xfree(m->src_cluster);
	xfree(m->src_user_name);
	xfree(m->remote_user_name);
	xfree(m->target_partition);
	xfree(m->app_name);
	xfree(m->script_path);
	xfree(m);
}

void brokerd_free_broker_ack_msg(brokerd_broker_ack_msg_t *m)
{
	if (!m)
		return;
	xfree(m->trace_id);
	xfree(m->dst_work_dir);
	xfree(m);
}

void brokerd_free_broker_staged_in_msg(brokerd_broker_staged_in_msg_t *m)
{
	if (!m)
		return;
	xfree(m->trace_id);
	xfree(m);
}

void brokerd_free_broker_submitted_msg(brokerd_broker_submitted_msg_t *m)
{
	if (!m)
		return;
	xfree(m->trace_id);
	xfree(m);
}

void brokerd_free_broker_query_status_msg(brokerd_broker_query_status_msg_t *m)
{
	if (!m)
		return;
	if (m->trace_ids) {
		for (uint32_t i = 0; i < m->trace_id_count; i++)
			xfree(m->trace_ids[i]);
		xfree(m->trace_ids);
	}
	xfree(m);
}

void brokerd_free_broker_status_msg(brokerd_broker_status_msg_t *m)
{
	if (!m)
		return;
	if (m->entries) {
		for (uint32_t i = 0; i < m->entry_count; i++) {
			xfree(m->entries[i].trace_id);
			xfree(m->entries[i].remote_alloc_tres);
		}
		xfree(m->entries);
	}
	xfree(m);
}

void brokerd_free_broker_cancel_msg(brokerd_broker_cancel_msg_t *m)
{
	if (!m)
		return;
	xfree(m->trace_id);
	xfree(m);
}

void brokerd_free_broker_cleanup_msg(brokerd_broker_cleanup_msg_t *m)
{
	if (!m)
		return;
	xfree(m->trace_id);
	xfree(m);
}

/* LEGACY_M04_TRANSITIONAL */
void brokerd_free_broker_remote_state_msg(brokerd_broker_remote_state_msg_t *m)
{
	if (!m)
		return;
	xfree(m->trace_id);
	xfree(m->remote_cluster_name);
	xfree(m->remote_partition_name);
	xfree(m->remote_alloc_tres);
	xfree(m);
}

/* LEGACY_M04_TRANSITIONAL */
void brokerd_free_broker_terminal_state_msg(
	brokerd_broker_terminal_state_msg_t *m)
{
	if (!m)
		return;
	/* base lives inline; free the strings inside, then the wrapper. */
	xfree(m->base.trace_id);
	xfree(m->base.remote_cluster_name);
	xfree(m->base.remote_partition_name);
	xfree(m->base.remote_alloc_tres);
	xfree(m);
}

void brokerd_free_msg_data(uint16_t msg_type, void *data)
{
	if (!data)
		return;

	switch (msg_type) {
	/* LEGACY_M04_TRANSITIONAL: 4 ctld<->broker cases (M05 PR removes). */
	case BROKERD_REQUEST_FORWARD_JOB:
		brokerd_free_forward_job_msg(data);
		break;
	case BROKERD_RESPONSE_FORWARD_JOB:
		brokerd_free_forward_job_resp_msg(data);
		break;
	case BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE:
		brokerd_free_broker_remote_state_msg(data);
		break;
	case BROKERD_REQUEST_BROKER_TERMINAL_STATE:
		brokerd_free_broker_terminal_state_msg(data);
		break;
	/* PERMANENT: 8 broker<->broker cases. */
	case BROKERD_REQUEST_BROKER_FORWARD_JOB:
		brokerd_free_broker_forward_job_msg(data);
		break;
	case BROKERD_RESPONSE_BROKER_ACK:
		brokerd_free_broker_ack_msg(data);
		break;
	case BROKERD_REQUEST_BROKER_STAGED_IN:
		brokerd_free_broker_staged_in_msg(data);
		break;
	case BROKERD_RESPONSE_BROKER_SUBMITTED:
		brokerd_free_broker_submitted_msg(data);
		break;
	case BROKERD_REQUEST_BROKER_QUERY_STATUS:
		brokerd_free_broker_query_status_msg(data);
		break;
	case BROKERD_RESPONSE_BROKER_STATUS:
		brokerd_free_broker_status_msg(data);
		break;
	case BROKERD_REQUEST_BROKER_CANCEL:
		brokerd_free_broker_cancel_msg(data);
		break;
	case BROKERD_REQUEST_BROKER_CLEANUP:
		brokerd_free_broker_cleanup_msg(data);
		break;
	default:
		warning("%s: unknown msg_type %hu, leaking payload pointer",
		        __func__, msg_type);
		break;
	}
}

/*****************************************************************************\
 *                       brokerd_strerror
\*****************************************************************************/

const char *brokerd_strerror(int rc)
{
	switch (rc) {
	case BROKERD_ERR_OVERLOAD:
		return "broker: in-flight job limit exceeded (BROKERD_ERR_OVERLOAD / 在途作业超限)";
	case BROKERD_ERR_NO_USER_MAPPING:
		return "broker: no user mapping for source user (BROKERD_ERR_NO_USER_MAPPING / 缺少用户映射)";
	case BROKERD_ERR_USER_MAPPING_MISMATCH:
		return "broker: user mapping fields inconsistent (BROKERD_ERR_USER_MAPPING_MISMATCH / 用户映射字段不一致)";
	case BROKERD_ERR_HOP_EXCEEDED:
		return "broker: cross-domain hop count exceeded (BROKERD_ERR_HOP_EXCEEDED / 跨域跳数超限)";
	case BROKERD_ERR_LOOKUP_FAILED:
		return "broker: lookup_software.sh failed (BROKERD_ERR_LOOKUP_FAILED / 软件路径解析失败)";
	case BROKERD_ERR_LOOKUP_TIMEOUT:
		return "broker: lookup_software.sh timeout (BROKERD_ERR_LOOKUP_TIMEOUT / 软件路径解析超时)";
	case BROKERD_ERR_STAGE_FAILED:
		return "broker: stage worker (rsync) failed (BROKERD_ERR_STAGE_FAILED / 数据传输失败)";
	case BROKERD_ERR_REMOTE_SUBMIT_FAILED:
		return "broker: remote sbatch submit rejected (BROKERD_ERR_REMOTE_SUBMIT_FAILED / 远端提交被拒)";
	case BROKERD_ERR_NOT_FOUND:
		return "broker: trace_id not found in table (BROKERD_ERR_NOT_FOUND / trace_id 未找到)";
	default:
		return slurm_strerror(rc);
	}
}

/*****************************************************************************\
 *                       wire frame helpers (public)
 *
 * Both directions of the broker private wire are exported here so that
 * the M05 listener (peer port 8443) and any future module that
 * sends/receives broker<->broker frames can reuse the exact same
 * encoder/decoder, keeping wire format consistency in one place.
\*****************************************************************************/

/*
 * Append the broker wire frame for (msg_type, payload) onto an outgoing
 * buf_t. Returns SLURM_SUCCESS on success.
 *
 * On failure, the buffer may contain partial bytes; the caller must
 * discard it instead of sending. We do not roll back the offset because
 * the buffer is single-shot per request.
 */
int brokerd_wire_build(buf_t *buffer, uint16_t msg_type, void *payload,
		       uint16_t protocol_version)
{
	void *cred = NULL;

	pack32(BROKERD_WIRE_MAGIC, buffer);
	pack16(protocol_version, buffer);
	pack16(msg_type, buffer);

	cred = auth_g_create(AUTH_DEFAULT_INDEX, NULL, SLURM_AUTH_UID_ANY,
			     NULL, 0);
	if (!cred) {
		error("%s: auth_g_create failed for msg_type=%s",
		      __func__, brokerd_msg_type_str(msg_type));
		return SLURM_ERROR;
	}
	if (auth_g_pack(cred, buffer, protocol_version)) {
		error("%s: auth_g_pack failed", __func__);
		auth_g_destroy(cred);
		return SLURM_ERROR;
	}
	auth_g_destroy(cred);

	if (brokerd_pack_msg(msg_type, payload, protocol_version, buffer)) {
		error("%s: pack payload failed for msg_type=%s",
		      __func__, brokerd_msg_type_str(msg_type));
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

/*
 * Decode a broker wire frame. On success:
 *   *msg_type_out          = the sender's msg_type
 *   *protocol_version_out  = the sender's protocol_version
 *   *payload_out           = xmalloc'd payload (free with brokerd_free_msg_data)
 *
 * If the frame fails magic / auth verification or the payload cannot be
 * decoded, all intermediates are released and SLURM_ERROR is returned
 * with errno left as-is when meaningful (set by the caller's I/O layer).
 */
int brokerd_wire_parse(buf_t *buffer,
		       uint16_t *msg_type_out,
		       uint16_t *protocol_version_out,
		       void **payload_out)
{
	uint32_t magic = 0;
	uint16_t pv = 0;
	uint16_t mtype = 0;
	void *cred = NULL;
	void *payload = NULL;

	*msg_type_out = 0;
	*protocol_version_out = 0;
	*payload_out = NULL;

	if (unpack32(&magic, buffer) || magic != BROKERD_WIRE_MAGIC) {
		error("%s: bad wire magic 0x%08x (expected 0x%08x)",
		      __func__, magic, BROKERD_WIRE_MAGIC);
		return SLURM_ERROR;
	}
	if (unpack16(&pv, buffer)) {
		error("%s: short read on protocol_version", __func__);
		return SLURM_ERROR;
	}
	if (unpack16(&mtype, buffer)) {
		error("%s: short read on msg_type", __func__);
		return SLURM_ERROR;
	}

	cred = auth_g_unpack(buffer, pv);
	if (!cred) {
		error("%s: auth_g_unpack failed", __func__);
		return SLURM_ERROR;
	}
	if (auth_g_verify(cred, NULL)) {
		error("%s: auth_g_verify rejected credential", __func__);
		auth_g_destroy(cred);
		return SLURM_ERROR;
	}
	auth_g_destroy(cred);

	if (brokerd_unpack_msg(mtype, &payload, pv, buffer)) {
		error("%s: brokerd_unpack_msg failed for msg_type=%s",
		      __func__, brokerd_msg_type_str(mtype));
		return SLURM_ERROR;
	}

	*msg_type_out         = mtype;
	*protocol_version_out = pv;
	*payload_out          = payload;
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       proto_send_recv_to_peer
\*****************************************************************************/

int proto_send_recv_to_peer(uint16_t msg_type, void *req,
			    int timeout_s, uint16_t resp_type,
			    void **resp_out)
{
	buf_t *send_buf = NULL;
	buf_t *recv_buf = NULL;
	char *raw = NULL;
	size_t raw_len = 0;
	int fd = -1;
	int rc = SLURM_ERROR;
	int timeout_ms;
	uint16_t got_msg_type = 0;
	uint16_t got_pv = 0;
	void *payload = NULL;
	ssize_t sent;

	if (!resp_out) {
		error("%s: NULL resp_out", __func__);
		return SLURM_ERROR;
	}
	*resp_out = NULL;

	if (!proto_inited) {
		error("%s: proto layer not initialised", __func__);
		return SLURM_ERROR;
	}

	timeout_ms = (timeout_s > 0) ? (timeout_s * 1000)
				     : BROKERD_DEFAULT_TIMEOUT_MS;

	/* 1. build outbound frame */
	send_buf = init_buf(BUF_SIZE);
	if (!send_buf) {
		error("%s: init_buf failed", __func__);
		return SLURM_ERROR;
	}
	if (brokerd_wire_build(send_buf, msg_type, req,
			      SLURM_PROTOCOL_VERSION) != SLURM_SUCCESS) {
		FREE_NULL_BUFFER(send_buf);
		return SLURM_ERROR;
	}

	/* 2. open TCP connection */
	fd = slurm_open_msg_conn(&g_peer_addr);
	if (fd < 0) {
		error("%s: connect to %s:%u failed: %m",
		      __func__, g_peer_host, g_peer_port);
		FREE_NULL_BUFFER(send_buf);
		return SLURM_ERROR;
	}

	/* 3. write request */
	sent = slurm_msg_sendto(fd, get_buf_data(send_buf),
				get_buf_offset(send_buf));
	FREE_NULL_BUFFER(send_buf);
	if (sent < 0) {
		error("%s: slurm_msg_sendto to %s:%u failed: %m",
		      __func__, g_peer_host, g_peer_port);
		(void) close(fd);
		return SLURM_ERROR;
	}

	/* 4. read response (length-prefixed by the peer's slurm_msg_sendto) */
	if (slurm_msg_recvfrom_timeout(fd, &raw, &raw_len, timeout_ms) < 0) {
		int saved = errno;
		error("%s: recv from %s:%u failed: %s",
		      __func__, g_peer_host, g_peer_port, strerror(saved));
		(void) close(fd);
		errno = saved;
		return SLURM_ERROR;
	}
	(void) close(fd);

	/* 5. parse + verify */
	recv_buf = create_buf(raw, raw_len);
	if (!recv_buf) {
		error("%s: create_buf for recv failed", __func__);
		xfree(raw);
		return SLURM_ERROR;
	}
	rc = brokerd_wire_parse(recv_buf, &got_msg_type, &got_pv, &payload);
	FREE_NULL_BUFFER(recv_buf);
	if (rc) {
		error("%s: response from %s:%u failed to decode",
		      __func__, g_peer_host, g_peer_port);
		return SLURM_ERROR;
	}

	if (got_msg_type != resp_type) {
		error("%s: expected resp msg_type=%s got %s",
		      __func__, brokerd_msg_type_str(resp_type),
		      brokerd_msg_type_str(got_msg_type));
		brokerd_free_msg_data(got_msg_type, payload);
		return SLURM_ERROR;
	}
	(void) got_pv; /* protocol version negotiation comes in v0.2 */

	*resp_out = payload;
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       lifecycle
\*****************************************************************************/

int proto_init(void)
{
	if (proto_inited) {
		debug("%s: already initialised", __func__);
		return SLURM_SUCCESS;
	}

	if (!g_broker_conf.remote_broker_host ||
	    !g_broker_conf.remote_broker_port) {
		error("%s: remote broker peer not configured", __func__);
		return SLURM_ERROR;
	}

	xfree(g_peer_host);
	g_peer_host = xstrdup(g_broker_conf.remote_broker_host);
	g_peer_port = g_broker_conf.remote_broker_port;

	memset(&g_peer_addr, 0, sizeof(g_peer_addr));
	slurm_set_addr(&g_peer_addr, g_peer_port, g_peer_host);

	proto_inited = true;
	info("proto: peer endpoint = %s:%u", g_peer_host, g_peer_port);
	return SLURM_SUCCESS;
}

void proto_fini(void)
{
	if (!proto_inited)
		return;

	xfree(g_peer_host);
	g_peer_port = 0;
	memset(&g_peer_addr, 0, sizeof(g_peer_addr));
	proto_inited = false;
}
