/*****************************************************************************\
 *  listener.c - dual-port single-threaded RPC listener for slurmbrokerd.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See listener.h for the
 *  contract and doc/checklists/M05-listener.md §6.M05-T1..T4 for the
 *  task-by-task design.
 *
 *  Threading model
 *  ---------------
 *  Single listener thread, one select(2) over both listen fds with a
 *  1s timeout so listener_running can be polled cheaply. Per-connection
 *  work runs synchronously inline; throughput target is ~50 RPC/s,
 *  well below what one thread + slurm_receive_msg can sustain.
 *
 *  ACL (M05-T4)
 *  ------------
 *  * ctld port: only loopback (127.0.0.1 / ::1) accepted.
 *  * peer port: only IP addresses that g_broker_conf.remote_broker_host
 *               currently resolves to. DNS results are cached on first
 *               success and refreshed lazily on cache miss; brief DNS
 *               outages therefore do not lock the peer out.
\*****************************************************************************/

#include "config.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/fd.h"
#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/pack.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/slurm_protocol_socket.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "broker_conf.h"
#include "handler_ctld.h"
#include "handler_remote.h"
#include "listener.h"
#include "proto.h"

#ifndef MAX
#define MAX(a, b) (((a) > (b)) ? (a) : (b))
#endif

/*****************************************************************************\
 *                       module-local state
\*****************************************************************************/

typedef enum {
	SRC_CTLD = 0,
	SRC_PEER = 1,
} src_t;

static pthread_t listener_tid;
static int       listen_fd_ctld     = -1;
static int       listen_fd_peer     = -1;
static volatile bool listener_running = false;
static bool      listener_thread_active = false;

/*
 * Self-pipe trick: listener_stop writes one byte to wakeup_pipe[1] to
 * unblock select() instantly, instead of relying on close(listen_fd) to
 * do it. Two reasons we cannot just close the listen fds first:
 *
 *   1. glibc/Linux does NOT promise that closing a fd unblocks a
 *      select() that is currently waiting on it.
 *   2. fd-reuse race: between close(listen_fd) and select() returning,
 *      the OS may hand the same numeric fd to another thread; the
 *      listener could then "wake up" on a fd that is not actually a
 *      listening socket anymore, causing undefined behaviour.
 *
 * The pipe's read end is included in select's read set; on shutdown we
 * write one byte, drain it inside the loop, then exit cleanly. Listen
 * fds get closed only after pthread_join, when no thread can possibly
 * be inside select() anymore.
 */
static int  wakeup_pipe[2] = { -1, -1 };
#define WAKEUP_PIPE_RD  wakeup_pipe[0]
#define WAKEUP_PIPE_WR  wakeup_pipe[1]

/* Per-connection accept timeout when waiting for the first request
 * byte. Matches M05-T2 design (10s). */
#define LISTENER_RECV_TIMEOUT_MS  10000

/*****************************************************************************\
 *                       ACL helpers
\*****************************************************************************/

/*
 * Normalise an IPv4-mapped IPv6 address ("::ffff:127.0.0.1") down to
 * its v4 form so the loopback check stays simple.
 */
static void _normalise_ipstr(char *ipstr)
{
	const char *prefix = "::ffff:";
	size_t plen = strlen(prefix);

	if (strncasecmp(ipstr, prefix, plen) == 0)
		memmove(ipstr, ipstr + plen, strlen(ipstr) - plen + 1);
}

static bool _is_loopback(const char *ipstr)
{
	return (!strcmp(ipstr, "127.0.0.1") || !strcmp(ipstr, "::1"));
}

/* DNS cache for peer ACL. We fall back to the cache when getaddrinfo()
 * fails so a transient resolver hiccup does not lock the peer broker
 * out of its 8443 channel. */
static char **g_peer_ip_cache       = NULL;
static uint32_t g_peer_ip_cache_len = 0;

static void _cache_replace(char **new_ips, uint32_t new_len)
{
	if (g_peer_ip_cache) {
		for (uint32_t i = 0; i < g_peer_ip_cache_len; i++)
			xfree(g_peer_ip_cache[i]);
		xfree(g_peer_ip_cache);
	}
	g_peer_ip_cache     = new_ips;
	g_peer_ip_cache_len = new_len;
}

static void _refresh_peer_ip_cache(void)
{
	struct addrinfo hints = { 0 };
	struct addrinfo *res = NULL, *p;
	char **collected = NULL;
	uint32_t count = 0, cap = 0;

	if (!g_broker_conf.remote_broker_host)
		return;

	hints.ai_family   = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if (getaddrinfo(g_broker_conf.remote_broker_host, NULL,
			&hints, &res)) {
		debug("listener: getaddrinfo(%s) failed; keeping cached peer IPs",
		      g_broker_conf.remote_broker_host);
		return;
	}

	for (p = res; p; p = p->ai_next) {
		char tmp[INET6_ADDRSTRLEN] = "";

		if (p->ai_family == AF_INET) {
			struct sockaddr_in *s = (void *) p->ai_addr;
			inet_ntop(AF_INET, &s->sin_addr, tmp, sizeof(tmp));
		} else if (p->ai_family == AF_INET6) {
			struct sockaddr_in6 *s = (void *) p->ai_addr;
			inet_ntop(AF_INET6, &s->sin6_addr, tmp, sizeof(tmp));
		} else {
			continue;
		}
		if (count == cap) {
			cap = cap ? cap * 2 : 4;
			collected = xrealloc(collected, cap * sizeof(char *));
		}
		collected[count++] = xstrdup(tmp);
	}
	freeaddrinfo(res);

	if (count) {
		_cache_replace(collected, count);
		debug2("listener: peer IP cache refreshed (%u entries)", count);
	} else if (collected) {
		xfree(collected);
	}
}

static bool _peer_ip_allowed(const char *ipstr)
{
	for (uint32_t i = 0; i < g_peer_ip_cache_len; i++) {
		if (!strcmp(g_peer_ip_cache[i], ipstr))
			return true;
	}
	/* Cache miss: try a fresh resolve once before rejecting. */
	_refresh_peer_ip_cache();
	for (uint32_t i = 0; i < g_peer_ip_cache_len; i++) {
		if (!strcmp(g_peer_ip_cache[i], ipstr))
			return true;
	}
	return false;
}

/*
 * Accept a connection and apply src-specific ACL. Returns the new fd
 * on success, -1 on EAGAIN/EINTR, or -1 with the conn closed on ACL
 * rejection. peer_addr is filled in on success.
 */
static int _accept_with_acl(int listen_fd, src_t src,
			    slurm_addr_t *peer_addr)
{
	int conn_fd;
	struct sockaddr_storage peer = { 0 };
	socklen_t plen = sizeof(peer);
	char ipstr[INET6_ADDRSTRLEN] = "";

	conn_fd = slurm_accept_msg_conn(listen_fd, peer_addr);
	if (conn_fd < 0) {
		if (errno != EINTR)
			debug("listener: accept on %s fd: %m",
			      src == SRC_CTLD ? "ctld" : "peer");
		return -1;
	}

	/* Re-derive the peer IP via getpeername for ACL: the slurm_addr_t
	 * filled by slurm_accept_msg_conn is wrapped, but raw sockaddr is
	 * the canonical source for inet_ntop. */
	if (getpeername(conn_fd, (struct sockaddr *) &peer, &plen)) {
		warning("listener: getpeername failed: %m, dropping conn");
		(void) close(conn_fd);
		return -1;
	}
	if (peer.ss_family == AF_INET) {
		struct sockaddr_in *s = (void *) &peer;
		inet_ntop(AF_INET, &s->sin_addr, ipstr, sizeof(ipstr));
	} else if (peer.ss_family == AF_INET6) {
		struct sockaddr_in6 *s = (void *) &peer;
		inet_ntop(AF_INET6, &s->sin6_addr, ipstr, sizeof(ipstr));
	}
	_normalise_ipstr(ipstr);

	if (src == SRC_CTLD) {
		if (!_is_loopback(ipstr)) {
			warning("listener: rejecting non-local ctld conn from %s",
			        ipstr);
			(void) close(conn_fd);
			return -1;
		}
	} else {
		if (!_peer_ip_allowed(ipstr)) {
			warning("listener: rejecting peer conn from %s (not %s)",
			        ipstr,
			        g_broker_conf.remote_broker_host
				    ? g_broker_conf.remote_broker_host
				    : "(unset)");
			(void) close(conn_fd);
			return -1;
		}
	}
	return conn_fd;
}

/*****************************************************************************\
 *                       per-connection handlers
 *
 * The two paths are intentionally NOT collapsed into one helper because
 * they speak different wire protocols (slurm-native for ctld, broker
 * private for peer). Sharing logic here would obscure that contract.
\*****************************************************************************/

static void _handle_ctld_conn(int conn_fd)
{
	slurm_msg_t msg;
	int rc;

	slurm_msg_t_init(&msg);
	msg.conn_fd = conn_fd;

	rc = slurm_receive_msg(conn_fd, &msg, LISTENER_RECV_TIMEOUT_MS);
	if (rc) {
		error("listener[ctld]: slurm_receive_msg: %s",
		      slurm_strerror(rc));
		/* Try to send an RC reply so the agent on the ctld side can
		 * pick up a non-zero return; if conn is already half-closed
		 * the send is a no-op. */
		(void) slurm_send_rc_msg(&msg, rc);
		goto out;
	}
	dispatch_ctld_msg(&msg);
out:
	slurm_free_msg_members(&msg);
}

static void _handle_peer_conn(int conn_fd, slurm_addr_t *peer_addr)
{
	char *raw = NULL;
	size_t raw_len = 0;
	buf_t *buf = NULL;
	uint16_t msg_type = 0;
	uint16_t pv = 0;
	void *payload = NULL;

	if (slurm_msg_recvfrom_timeout(conn_fd, &raw, &raw_len,
				       LISTENER_RECV_TIMEOUT_MS) < 0) {
		debug("listener[peer]: recv timeout/error: %m");
		return;
	}

	buf = create_buf(raw, raw_len);
	if (!buf) {
		error("listener[peer]: create_buf failed");
		xfree(raw);
		return;
	}

	if (brokerd_wire_parse(buf, &msg_type, &pv, &payload)) {
		error("listener[peer]: wire frame decode failed");
		FREE_NULL_BUFFER(buf);
		return;
	}
	FREE_NULL_BUFFER(buf);
	(void) pv;  /* protocol version negotiation is v0.2 */

	dispatch_remote_msg(msg_type, payload, conn_fd, peer_addr);
	/* dispatch_remote_msg + handler take ownership of payload. */
}

/*****************************************************************************\
 *                       main loop
\*****************************************************************************/

/* Drain everything currently buffered in the wakeup pipe so the next
 * select() iteration does not spuriously fire. */
static void _drain_wakeup_pipe(void)
{
	char tmp[64];
	ssize_t n;

	do {
		n = read(WAKEUP_PIPE_RD, tmp, sizeof(tmp));
	} while (n > 0 || (n < 0 && errno == EINTR));
	/* nonblocking pipe: read returns -1/EAGAIN once empty, that's fine. */
}

static void *_listener_main(void *arg)
{
	(void) arg;

	while (listener_running) {
		fd_set rfds;
		struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
		int max_fd, n;

		FD_ZERO(&rfds);
		if (listen_fd_ctld >= 0)
			FD_SET(listen_fd_ctld, &rfds);
		if (listen_fd_peer >= 0)
			FD_SET(listen_fd_peer, &rfds);
		if (WAKEUP_PIPE_RD >= 0)
			FD_SET(WAKEUP_PIPE_RD, &rfds);
		max_fd = MAX(listen_fd_ctld, listen_fd_peer);
		max_fd = MAX(max_fd, WAKEUP_PIPE_RD);
		if (max_fd < 0)
			break;

		n = select(max_fd + 1, &rfds, NULL, NULL, &tv);
		if (n == 0)
			continue; /* 1s tick, re-check listener_running */
		if (n < 0) {
			if (errno == EINTR)
				continue;
			error("listener: select: %m");
			break;
		}

		/*
		 * Wakeup pipe handled FIRST so a stop request in flight
		 * preempts further accept work. Drain whatever bytes the
		 * stop side wrote (one byte per stop) and let the while
		 * condition re-check listener_running on the next iteration.
		 */
		if (WAKEUP_PIPE_RD >= 0 &&
		    FD_ISSET(WAKEUP_PIPE_RD, &rfds)) {
			_drain_wakeup_pipe();
			continue;
		}

		if (listen_fd_ctld >= 0 &&
		    FD_ISSET(listen_fd_ctld, &rfds)) {
			slurm_addr_t peer = { 0 };
			int conn_fd = _accept_with_acl(listen_fd_ctld,
						       SRC_CTLD, &peer);
			if (conn_fd >= 0) {
				_handle_ctld_conn(conn_fd);
				(void) close(conn_fd);
			}
		}
		if (listen_fd_peer >= 0 &&
		    FD_ISSET(listen_fd_peer, &rfds)) {
			slurm_addr_t peer = { 0 };
			int conn_fd = _accept_with_acl(listen_fd_peer,
						       SRC_PEER, &peer);
			if (conn_fd >= 0) {
				_handle_peer_conn(conn_fd, &peer);
				(void) close(conn_fd);
			}
		}
	}
	return NULL;
}

/*****************************************************************************\
 *                       lifecycle
\*****************************************************************************/

int listener_start(void)
{
	if (listener_thread_active) {
		debug("%s: already started", __func__);
		return SLURM_SUCCESS;
	}
	if (!g_broker_conf.ctld_port || !g_broker_conf.peer_port) {
		error("listener: ctld_port or peer_port not configured");
		return SLURM_ERROR;
	}

	/*
	 * Create the wakeup pipe FIRST so it is ready before the worker
	 * thread enters select(). Both ends are nonblocking: the writer
	 * never blocks (a few stale wakeup bytes are harmless) and the
	 * drain loop on the read end can spin until EAGAIN.
	 */
	if (pipe(wakeup_pipe) < 0) {
		error("listener: pipe: %m");
		return SLURM_ERROR;
	}
	fd_set_nonblocking(WAKEUP_PIPE_RD);
	fd_set_nonblocking(WAKEUP_PIPE_WR);
	fd_set_close_on_exec(WAKEUP_PIPE_RD);
	fd_set_close_on_exec(WAKEUP_PIPE_WR);

	listen_fd_ctld = slurm_init_msg_engine_port(g_broker_conf.ctld_port);
	if (listen_fd_ctld < 0) {
		error("listener: bind ctld port %u: %m",
		      g_broker_conf.ctld_port);
		(void) close(WAKEUP_PIPE_RD); WAKEUP_PIPE_RD = -1;
		(void) close(WAKEUP_PIPE_WR); WAKEUP_PIPE_WR = -1;
		return SLURM_ERROR;
	}
	listen_fd_peer = slurm_init_msg_engine_port(g_broker_conf.peer_port);
	if (listen_fd_peer < 0) {
		error("listener: bind peer port %u: %m",
		      g_broker_conf.peer_port);
		(void) close(listen_fd_ctld); listen_fd_ctld = -1;
		(void) close(WAKEUP_PIPE_RD); WAKEUP_PIPE_RD = -1;
		(void) close(WAKEUP_PIPE_WR); WAKEUP_PIPE_WR = -1;
		return SLURM_ERROR;
	}

	/* Pre-warm the peer IP cache so the first inbound peer connection
	 * does not pay the DNS round-trip latency. Failure is non-fatal:
	 * the cache will lazily refill on first cache miss. */
	_refresh_peer_ip_cache();

	listener_running       = true;
	listener_thread_active = true;
	slurm_thread_create(&listener_tid, _listener_main, NULL);

	info("listener: listening on ctld_port=%u peer_port=%u",
	     g_broker_conf.ctld_port, g_broker_conf.peer_port);
	return SLURM_SUCCESS;
}

void listener_stop(void)
{
	if (!listener_thread_active)
		return;

	/*
	 * Order matters and avoids the close-fd-during-select race:
	 *   1. clear listener_running so the loop will exit on its next
	 *      iteration;
	 *   2. write 1 byte to the wakeup pipe so select() returns NOW
	 *      instead of waiting up to 1s for the timeout;
	 *   3. join the worker (it will see the wakeup, drain the pipe,
	 *      then notice listener_running == false and exit);
	 *   4. only AFTER the worker is gone do we close the listen fds
	 *      and the pipe -- no other thread can possibly be inside
	 *      select() at that point, so the kernel cannot reuse the
	 *      fd numbers under us.
	 */
	listener_running = false;

	if (WAKEUP_PIPE_WR >= 0) {
		const char b = 'q';
		ssize_t n;

		do {
			n = write(WAKEUP_PIPE_WR, &b, 1);
		} while (n < 0 && errno == EINTR);
		if (n < 0 && errno != EAGAIN)
			debug("listener: wakeup write: %m");
		/* Even if write fails, the 1s select timeout still bounds
		 * shutdown latency. */
	}

	(void) pthread_join(listener_tid, NULL);
	listener_thread_active = false;

	if (listen_fd_ctld >= 0) {
		(void) close(listen_fd_ctld);
		listen_fd_ctld = -1;
	}
	if (listen_fd_peer >= 0) {
		(void) close(listen_fd_peer);
		listen_fd_peer = -1;
	}
	if (WAKEUP_PIPE_RD >= 0) {
		(void) close(WAKEUP_PIPE_RD);
		WAKEUP_PIPE_RD = -1;
	}
	if (WAKEUP_PIPE_WR >= 0) {
		(void) close(WAKEUP_PIPE_WR);
		WAKEUP_PIPE_WR = -1;
	}

	_cache_replace(NULL, 0);

	info("listener: stopped");
}
