/*****************************************************************************\
 *  listener.h - dual-port single-threaded RPC listener for slurmbrokerd.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/checklists/M05-listener.md
 *  for the full design and doc/Broker详细设计文档MVP.md §3.3 / §8.3.
 *
 *  Two listening sockets share one select-driven thread:
 *
 *      g_broker_conf.ctld_port (8442)    -> slurm-native frame
 *                                            slurm_receive_msg() +
 *                                            dispatch_ctld_msg()
 *
 *      g_broker_conf.peer_port (8443)    -> broker private wire frame
 *                                            slurm_msg_recvfrom_timeout +
 *                                            brokerd_wire_parse +
 *                                            dispatch_remote_msg()
 *
 *  Per the M04 §10/§11 split, the ctld port path expects the slurmctld
 *  engineer's PR to register the 4 ctld<->broker msg_types in
 *  src/common/. Until that lands, slurm_receive_msg() rejects the
 *  unknown msg_type and replies ESLURM_PROTOCOL_INVALID_MESSAGE; the
 *  listener stays alive and the peer port is unaffected.
\*****************************************************************************/

#ifndef _BROKERD_LISTENER_H
#define _BROKERD_LISTENER_H

extern int  listener_start(void);
extern void listener_stop(void);

#endif /* _BROKERD_LISTENER_H */
