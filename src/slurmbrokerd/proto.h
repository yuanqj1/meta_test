/*****************************************************************************\
 *  proto.h - slurmbrokerd RPC msg_type / payload / pack-unpack contract.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/Broker详细设计文档MVP.md
 *  §6 and doc/checklists/M04-rpc.md for design.
 *
 *  This header is the **single source of truth** for the 11 broker
 *  message type IDs, the 9 broker error codes, and every payload struct
 *  used between {ctld <-> broker} and {broker <-> broker}. All other
 *  modules (M05/M06/M07/M08/M13) MUST include this header rather than
 *  redefining the wire contract.
 *
 *  Workspace rule: native Slurm sources are off-limits. The broker
 *  therefore reserves its own msg_type / errno ranges (8000-8099 /
 *  9001-9099) inside this header instead of slurm_protocol_defs.h /
 *  slurm_errno.h, and ships its own pack/unpack dispatcher in
 *  proto_pack.c. The wire format borrows Slurm's pack8/16/32/packstr
 *  primitives so the layout matches what a future ctld-side patch
 *  would produce when it does land in slurm_protocol_pack.c.
\*****************************************************************************/

#ifndef _BROKERD_PROTO_H
#define _BROKERD_PROTO_H

#include <inttypes.h>
#include <stdbool.h>
#include <time.h>

#include "slurm/slurm.h"

#include "src/common/pack.h"

/*****************************************************************************\
 *                       msg_type IDs (8000-8099 reserved)
 *
 * Numbers are wire-significant. Adding new types must extend at the tail
 * to preserve compatibility with already-deployed peers.
\*****************************************************************************/

/*
 * ctld <-> broker (local munge socket)
 *
 * LEGACY_M04_TRANSITIONAL: these 4 macros + the corresponding payload
 * structs / pack / unpack / free implementations are the broker-side
 * fallback so M04 PR can run end-to-end mock tests on its own. Per the
 * design in checklists/M04-rpc.md §10, the long-term path is for the
 * slurmctld engineer to register these msg types in
 * src/common/slurm_protocol_defs.h (no broker prefix); the broker M05
 * listener PR will then delete every block tagged
 * "LEGACY_M04_TRANSITIONAL" and consume the slurm-native struct
 * directly via slurm_msg_t.data.
 *
 * The numeric values 8001-8004 below MUST stay equal to whatever the
 * slurmctld PR ends up registering in src/common/.
 */
#define BROKERD_REQUEST_FORWARD_JOB                 8001 /* LEGACY_M04_TRANSITIONAL */
#define BROKERD_RESPONSE_FORWARD_JOB                8002 /* LEGACY_M04_TRANSITIONAL */
#define BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE  8003 /* LEGACY_M04_TRANSITIONAL */
#define BROKERD_REQUEST_BROKER_TERMINAL_STATE       8004 /* LEGACY_M04_TRANSITIONAL */

/*
 * broker <-> broker (peer port). These 8 macros are PERMANENT; cross-
 * cluster RPCs always use the broker-private wire frame and are not
 * registered with slurm-native pack_msg() under any roadmap.
 *
 * Note: 8016 (REQUEST_BROKER_CANCEL) is dual-purpose - the slurmctld PR
 * will register the same numeric value (8016) under the unprefixed name
 * REQUEST_BROKER_CANCEL for the ctld -> broker direction; the broker
 * keeps BROKERD_REQUEST_BROKER_CANCEL for its private broker -> broker
 * propagation path. Both names hold the same uint16_t value so a single
 * payload struct (with identical field order) works for both paths.
 */
#define BROKERD_REQUEST_BROKER_FORWARD_JOB          8010
#define BROKERD_RESPONSE_BROKER_ACK                 8011
#define BROKERD_REQUEST_BROKER_STAGED_IN            8012
#define BROKERD_RESPONSE_BROKER_SUBMITTED           8013
#define BROKERD_REQUEST_BROKER_QUERY_STATUS         8014
#define BROKERD_RESPONSE_BROKER_STATUS              8015
#define BROKERD_REQUEST_BROKER_CANCEL               8016
#define BROKERD_REQUEST_BROKER_CLEANUP              8017

/*****************************************************************************\
 *                       broker-local error codes (9001-9099)
\*****************************************************************************/

#define BROKERD_ERR_OVERLOAD                  9001 /* > MaxInFlightJobs       */
#define BROKERD_ERR_NO_USER_MAPPING           9002
#define BROKERD_ERR_USER_MAPPING_MISMATCH     9003
#define BROKERD_ERR_HOP_EXCEEDED              9004
#define BROKERD_ERR_LOOKUP_FAILED             9005
#define BROKERD_ERR_LOOKUP_TIMEOUT            9006
#define BROKERD_ERR_STAGE_FAILED              9007
#define BROKERD_ERR_REMOTE_SUBMIT_FAILED      9008
#define BROKERD_ERR_NOT_FOUND                 9009

/*
 * Wire-frame magic (little-endian "BRKR" reads as 0x524B5242).
 * Anything that does not start with this magic is a stale or hostile
 * frame and is dropped without trying to decode further.
 */
#define BROKERD_WIRE_MAGIC 0x524B5242U

/*****************************************************************************\
 *                       payload structs (wire-significant order)
 *
 * Field order in each struct mirrors the order pack/unpack writes/reads
 * the wire. Do NOT reorder; append-only.
\*****************************************************************************/

/*
 * LEGACY_M04_TRANSITIONAL: ctld <-> broker structs.
 *
 * These mirror the (future) slurmctld-side payload structs that the
 * ctld engineer will register in src/common/slurm_protocol_defs.h. They
 * exist here purely so the broker can pack/unpack on its own private
 * wire frame for mock end-to-end tests until the ctld PR lands; M05
 * listener PR removes them in favour of consuming the slurm-native
 * structs directly via slurm_msg_t.data. See M04 §10 / §11.
 */

/* REQUEST_FORWARD_JOB : ctld -> broker (local munge) */
typedef struct {
	uint32_t   src_job_id;
	uint32_t   src_uid;
	uint32_t   src_gid;
	char      *src_user_name;
	char      *target_cluster;
	char      *src_work_dir;
	char      *script_path;
	char      *account;
	char      *app_name;          /* fed to lookup_software.sh later */
	job_desc_msg_t *job_desc;
} brokerd_forward_job_msg_t;      /* LEGACY_M04_TRANSITIONAL */

/* RESPONSE_FORWARD_JOB : broker -> ctld */
typedef struct {
	uint32_t   error_code;
	char      *trace_id;
} brokerd_forward_job_resp_msg_t; /* LEGACY_M04_TRANSITIONAL */

/*
 * REQUEST_BROKER_FORWARD_JOB : broker (orig) -> broker (recv).
 *
 * Slurm-version-INDEPENDENT payload. The receiver uses
 *   sudo -u <remote_user_name> sbatch \
 *        --partition=<target_partition> \
 *        --chdir=<dst_work_dir> \
 *        <dst_work_dir>/<basename(script_path)>
 * to submit, where the script's own #SBATCH header carries every
 * resource request (nodes / time / mem / ...). This avoids shipping a
 * job_desc_msg_t and keeps A=24.05 <-> B=23.11 cross-cluster traffic
 * compatible.
 */
typedef struct {
	char      *trace_id;
	uint8_t    hop_count;
	char      *src_cluster;
	uint32_t   src_job_id;
	char      *src_user_name;
	char      *remote_user_name;
	char      *target_partition;
	char      *app_name;          /* fed to lookup_software.sh on receiver */
	char      *script_path;       /* originator-side absolute path; receiver
				       * uses basename() after rsync delivers
				       * the file under dst_work_dir. */
} brokerd_broker_forward_job_msg_t;

/* RESPONSE_BROKER_ACK : broker (recv) -> broker (orig) */
typedef struct {
	uint32_t   error_code;
	char      *trace_id;
	char      *dst_work_dir;      /* receiver-created path */
} brokerd_broker_ack_msg_t;

/* REQUEST_BROKER_STAGED_IN : broker (orig) -> broker (recv) */
typedef struct {
	char      *trace_id;
} brokerd_broker_staged_in_msg_t;

/* RESPONSE_BROKER_SUBMITTED : broker (recv) -> broker (orig) */
typedef struct {
	uint32_t   error_code;
	char      *trace_id;
	uint32_t   remote_job_id;
} brokerd_broker_submitted_msg_t;

/* REQUEST_BROKER_QUERY_STATUS : broker (orig) -> broker (recv) */
typedef struct {
	uint32_t   trace_id_count;
	char     **trace_ids;
} brokerd_broker_query_status_msg_t;

/* Single status row inside RESPONSE_BROKER_STATUS */
typedef struct {
	char      *trace_id;
	uint32_t   remote_state;        /* JOB_PENDING|RUNNING|... */
	time_t     remote_start_time;
	time_t     remote_end_time;
	char      *remote_alloc_tres;
	int32_t    remote_exit_code;
} brokerd_broker_status_entry_t;

/* RESPONSE_BROKER_STATUS : broker (recv) -> broker (orig) */
typedef struct {
	uint32_t   entry_count;
	brokerd_broker_status_entry_t *entries;
} brokerd_broker_status_msg_t;

/* REQUEST_BROKER_CANCEL : ctld -> broker, broker -> broker */
typedef struct {
	uint32_t   src_job_id;        /* set when from ctld */
	char      *trace_id;          /* set when from peer broker */
} brokerd_broker_cancel_msg_t;

/* REQUEST_BROKER_CLEANUP : broker -> broker */
typedef struct {
	char      *trace_id;
} brokerd_broker_cleanup_msg_t;

/* REQUEST_BROKER_UPDATE_REMOTE_STATE : broker -> ctld */
typedef struct {
	uint32_t   src_job_id;
	char      *trace_id;
	char      *remote_cluster_name;
	char      *remote_partition_name;
	uint32_t   remote_job_id;
	uint32_t   remote_state;
	char      *remote_alloc_tres;
	time_t     remote_start_time;
} brokerd_broker_remote_state_msg_t; /* LEGACY_M04_TRANSITIONAL */

/* REQUEST_BROKER_TERMINAL_STATE : broker -> ctld
 * Wire-format = remote_state_msg fields followed by the two terminal-only
 * fields. Modelled as `base` to avoid duplication on the C side.
 */
typedef struct {
	brokerd_broker_remote_state_msg_t base;
	time_t     remote_end_time;
	int32_t    remote_exit_code;
} brokerd_broker_terminal_state_msg_t; /* LEGACY_M04_TRANSITIONAL */

/*****************************************************************************\
 *                       free helpers (per-msg + dispatcher)
\*****************************************************************************/

/* LEGACY_M04_TRANSITIONAL: 4 ctld<->broker free helpers (M05 PR removes). */
extern void brokerd_free_forward_job_msg(brokerd_forward_job_msg_t *m);
extern void brokerd_free_forward_job_resp_msg(brokerd_forward_job_resp_msg_t *m);
extern void brokerd_free_broker_remote_state_msg(
	brokerd_broker_remote_state_msg_t *m);
extern void brokerd_free_broker_terminal_state_msg(
	brokerd_broker_terminal_state_msg_t *m);

/* PERMANENT: 7 broker<->broker free helpers + dual-purpose 8016 cancel. */
extern void brokerd_free_broker_forward_job_msg(
	brokerd_broker_forward_job_msg_t *m);
extern void brokerd_free_broker_ack_msg(brokerd_broker_ack_msg_t *m);
extern void brokerd_free_broker_staged_in_msg(brokerd_broker_staged_in_msg_t *m);
extern void brokerd_free_broker_submitted_msg(brokerd_broker_submitted_msg_t *m);
extern void brokerd_free_broker_query_status_msg(
	brokerd_broker_query_status_msg_t *m);
extern void brokerd_free_broker_status_msg(brokerd_broker_status_msg_t *m);
extern void brokerd_free_broker_cancel_msg(brokerd_broker_cancel_msg_t *m);
extern void brokerd_free_broker_cleanup_msg(brokerd_broker_cleanup_msg_t *m);

/*
 * Dispatcher: free the payload pointer based on msg_type. Safe on NULL
 * payload. Unknown msg_type triggers a warning but never crashes; the
 * pointer is leaked rather than mis-freed, which matches the Slurm
 * native pattern in slurm_free_msg_data().
 */
extern void brokerd_free_msg_data(uint16_t msg_type, void *data);

/*****************************************************************************\
 *                       pack / unpack dispatcher
\*****************************************************************************/

/*
 * brokerd_pack_msg
 *   Append the wire-encoded payload for `msg_type` onto `buffer` using
 *   `protocol_version` to govern field selection.
 *   Returns SLURM_SUCCESS on success, SLURM_ERROR if msg_type is unknown
 *   or proto_ver is not supported.
 */
extern int brokerd_pack_msg(uint16_t msg_type, void *payload,
			    uint16_t protocol_version, buf_t *buffer);

/*
 * brokerd_wire_build
 *   Build a complete broker private wire frame onto `buffer`:
 *     [BRKR magic][proto_ver][msg_type][auth_g_pack blob][payload via
 *      brokerd_pack_msg].
 *   Caller hands the resulting buf_t to slurm_msg_sendto(). On failure
 *   the buffer may contain partial bytes; the caller must discard.
 *
 *   Returns SLURM_SUCCESS / SLURM_ERROR.
 */
extern int brokerd_wire_build(buf_t *buffer, uint16_t msg_type, void *payload,
			      uint16_t protocol_version);

/*
 * brokerd_wire_parse
 *   Decode a complete broker private wire frame from `buffer`. On
 *   success:
 *     *msg_type_out         = the sender's msg_type
 *     *protocol_version_out = the sender's protocol_version
 *     *payload_out          = xmalloc'd payload (free with
 *                             brokerd_free_msg_data(*msg_type_out, ...))
 *   On any decode/auth failure all intermediates are released.
 *
 *   Returns SLURM_SUCCESS / SLURM_ERROR.
 */
extern int brokerd_wire_parse(buf_t *buffer, uint16_t *msg_type_out,
			      uint16_t *protocol_version_out,
			      void **payload_out);

/*
 * brokerd_unpack_msg
 *   Decode a payload of `msg_type` from `buffer`. On success
 *   `*payload_out` points at a freshly xmalloc'd struct that the caller
 *   must release with brokerd_free_msg_data(msg_type, *payload_out).
 *   Returns SLURM_SUCCESS / SLURM_ERROR.
 */
extern int brokerd_unpack_msg(uint16_t msg_type, void **payload_out,
			      uint16_t protocol_version, buf_t *buffer);

/* Human-readable msg type, for logs only. Returns a const string; never
 * NULL even for unknown ids. */
extern const char *brokerd_msg_type_str(uint16_t msg_type);

/* Human-readable broker error code; falls back to slurm_strerror() for
 * non-broker codes. Never NULL. */
extern const char *brokerd_strerror(int rc);

/*****************************************************************************\
 *                       lifecycle + send_recv API
\*****************************************************************************/

extern int  proto_init(void);
extern void proto_fini(void);

/*
 * Synchronous request/response to the configured remote broker peer
 * (g_broker_conf.remote_broker_host:remote_broker_port).
 *
 *   msg_type   - one of BROKERD_REQUEST_BROKER_*
 *   req        - payload pointer (caller-owned)
 *   timeout_s  - wall-clock seconds for connect + send + recv combined
 *   resp_type  - expected response msg type; mismatch returns SLURM_ERROR
 *   resp_out   - on success set to xmalloc'd response payload that the
 *                caller must release with brokerd_free_msg_data(resp_type,
 *                *resp_out)
 *
 * Returns SLURM_SUCCESS / SLURM_ERROR / BROKERD_ERR_*.
 */
extern int proto_send_recv_to_peer(uint16_t msg_type, void *req,
				   int timeout_s, uint16_t resp_type,
				   void **resp_out);

#endif /* _BROKERD_PROTO_H */
