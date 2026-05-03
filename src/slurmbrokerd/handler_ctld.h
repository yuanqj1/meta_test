/*****************************************************************************\
 *  handler_ctld.h - dispatcher and per-msg stubs for the ctld -> broker
 *                   inbound RPC path (CtldPort 8442, slurm-native frame).
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/checklists/M05-listener.md
 *  §5 / §6 for the dispatch contract and doc/checklists/M04-rpc.md §10 / §11
 *  for the broker / slurmctld split-PR responsibility model.
 *
 *  Wire path
 *  ---------
 *  CtldPort traffic always uses the **slurm-native** RPC frame
 *  (slurm_msg_t + pack_msg/unpack_msg big switch). The actual unpack
 *  happens inside slurm_receive_msg() in the listener, before we get
 *  here; by the time dispatch_ctld_msg() is called, msg->msg_type and
 *  msg->data are populated by the slurm-native path.
 *
 *  Until the slurmctld-side PR registers the 4 ctld<->broker msg_type
 *  values in src/common/, slurm_receive_msg() will return
 *  SLURM_UNEXPECTED_MSG_ERROR when a real ctld attempts to talk to
 *  the broker on 8442. The listener handles that gracefully and the
 *  broker stays up. Once the ctld PR lands and libslurm.so is rebuilt,
 *  these handler stubs become reachable; M06 fills in the real bodies.
\*****************************************************************************/

#ifndef _BROKERD_HANDLER_CTLD_H
#define _BROKERD_HANDLER_CTLD_H

#include "slurm/slurm.h"

#include "src/common/slurm_protocol_defs.h"

/*
 * Dispatch a fully-decoded ctld -> broker request. The listener has
 * already authenticated the connection (peer is SlurmUser on
 * 127.0.0.1) and called slurm_receive_msg() to populate msg->msg_type
 * and msg->data. The handler is responsible for sending a reply
 * (via slurm_send_node_msg() for typed responses or slurm_send_rc_msg()
 * for plain return codes) before returning so the ctld agent thread
 * can collect the response.
 *
 * Ownership: msg.data is owned by msg; the listener calls
 * slurm_free_msg_members(msg) after this function returns.
 */
extern void dispatch_ctld_msg(slurm_msg_t *msg);

/*
 * Per-msg handlers (M06).
 *
 * Return value:
 *   SLURM_SUCCESS       - handler completed; reply already sent.
 *   SLURM_ERROR         - handler aborted before sending a reply (caller
 *                         should send a generic RC failure on its behalf).
 *
 * The handlers themselves never throw; any business-level rejection
 * (overload, no user mapping, ACL deny, duplicate trace_id, ...) is
 * communicated to the ctld via the wire-level reply with a non-zero
 * `error_code` field, while the function still returns SLURM_SUCCESS
 * because the dispatch contract was honoured.
 *
 * Numeric msg_type values are slurmctld-side definitions (the broker
 * does not redefine them); see M04 §10.5 for the segment allocation.
 */
extern int handle_forward_job(slurm_msg_t *msg);              /* 8001 */
extern int handle_cancel_from_ctld(slurm_msg_t *msg);         /* 8016 */

#endif /* _BROKERD_HANDLER_CTLD_H */
