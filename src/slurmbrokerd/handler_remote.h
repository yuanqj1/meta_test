/*****************************************************************************\
 *  handler_remote.h - dispatcher and per-msg stubs for the broker -> broker
 *                     inbound RPC path (PeerPort 8443, broker private wire).
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/checklists/M05-listener.md
 *  §5 / §6 for the dispatch contract and doc/checklists/M04-rpc.md §2.4 for
 *  the broker private wire format.
 *
 *  Wire path
 *  ---------
 *  PeerPort traffic always uses the **broker private** wire frame
 *  ('BRKR' magic + auth + payload). The listener calls
 *  brokerd_wire_parse() to populate (msg_type, payload, peer) before
 *  invoking dispatch_remote_msg(); responses (e.g. BROKERD_RESPONSE_*)
 *  are built with brokerd_wire_build() and sent back over the same
 *  conn_fd via slurm_msg_sendto().
 *
 *  This path is independent of any slurmctld-side change and stays
 *  PERMANENT under the M04 §10 split (cross-cluster RPCs never enter
 *  src/common/).
\*****************************************************************************/

#ifndef _BROKERD_HANDLER_REMOTE_H
#define _BROKERD_HANDLER_REMOTE_H

#include <inttypes.h>

#include "slurm/slurm.h"

/*
 * Dispatch a fully-decoded broker -> broker request.
 *
 * IN  msg_type  - BROKERD_REQUEST_BROKER_*
 * IN  payload   - xmalloc'd payload (typed per msg_type); ownership
 *                 transfers to the handler. The handler MUST release
 *                 it with brokerd_free_msg_data(msg_type, payload)
 *                 before returning.
 * IN  conn_fd   - peer socket fd; handler may write a response frame
 *                 to it via brokerd_wire_build + slurm_msg_sendto.
 * IN  peer_addr - opaque pointer to slurm_addr_t for logging only.
 */
extern void dispatch_remote_msg(uint16_t msg_type, void *payload,
				int conn_fd, slurm_addr_t *peer_addr);

/*
 * Per-msg stubs (M07 fills in the real bodies). Each stub takes
 * ownership of payload and conn_fd from dispatch_remote_msg().
 */
extern void handle_broker_forward_job(void *payload, int conn_fd);   /* 8010 */
extern void handle_broker_staged_in(void *payload, int conn_fd);     /* 8012 */
extern void handle_broker_query_status(void *payload, int conn_fd);  /* 8014 */
extern void handle_broker_cancel(void *payload, int conn_fd);        /* 8016 */
extern void handle_broker_cleanup(void *payload, int conn_fd);       /* 8017 */

#endif /* _BROKERD_HANDLER_REMOTE_H */
