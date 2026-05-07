/*****************************************************************************\
 *  inject_broker_forward.c - smoke-test client for slurmbrokerd peer port.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). NOT installed; built as a
 *  noinst_PROGRAMS target alongside slurmbrokerd for use during bring-up.
 *
 *  What this tool does
 *  -------------------
 *  Sends one BROKERD_REQUEST_BROKER_FORWARD_JOB frame to a target slurmbrokerd
 *  on its peer port (default 8443) and prints the BROKER_ACK response.
 *
 *  Why a separate tool
 *  -------------------
 *  The ctld -> broker direction (port 8442) uses slurm-native pack_msg /
 *  unpack_msg, but the broker's msg_type IDs (8001-8004) are not yet
 *  registered in src/common/. Until that sister PR lands, the only way to
 *  exercise the broker stack end-to-end is to hit the peer port, which
 *  uses the broker's private BRKR-magic frame and is fully self-contained.
 *
 *  This tool therefore validates:
 *    * peer port listener accept + ACL (must be RemoteBrokerHost's IP)
 *    * Munge auth_g_pack / auth_g_verify round-trip
 *    * brokerd_wire_build / brokerd_wire_parse (proto layer)
 *    * handler_remote.c::handle_broker_forward_job
 *      (hop check, user_mapping reverse-match, mkdir dst_work_dir)
 *
 *  Single-host smoke test
 *  ----------------------
 *  Configure broker.conf with RemoteBrokerHost=127.0.0.1 so the receiver-
 *  side ACL accepts our connection from loopback. Then:
 *
 *    ./inject_broker_forward \
 *        --peer-host 127.0.0.1 --peer-port 8443 \
 *        --trace-id  clusterA-100 \
 *        --src-cluster clusterA \
 *        --src-job-id 100 \
 *        --src-user   alice \
 *        --remote-user alice \
 *        --partition  normal \
 *        --app-name   gromacs \
 *        --script     /tmp/job.sh
 *
 *  Run as the same Munge realm as the target broker (typically root or
 *  SlurmUser); auth_g_pack will fail otherwise.
\*****************************************************************************/

#include "config.h"

#include <getopt.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/log.h"
#include "src/common/pack.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_common.h"
#include "src/common/slurm_protocol_socket.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "broker_conf.h"
#include "proto.h"

/*
 * proto.c references g_broker_conf inside proto_init / proto_send_*
 * which we never call from this tool. The linker still requires the
 * symbol because we statically link proto.o into the binary. Provide
 * a zero-initialised definition here; populating it is unnecessary.
 */
broker_conf_t g_broker_conf;

#define DEFAULT_PEER_PORT  8443
#define DEFAULT_TIMEOUT_S  10

typedef struct {
	char    *peer_host;
	uint16_t peer_port;
	int      timeout_s;
	int      verbose;

	char    *trace_id;
	char    *src_cluster;
	uint32_t src_job_id;
	char    *src_user;
	char    *remote_user;
	char    *partition;
	char    *app_name;
	char    *script;
	uint8_t  hop_count;
} args_t;

static void _usage(const char *prog)
{
	fprintf(stderr,
		"Usage: %s [options]\n"
		"\n"
		"Connection (required):\n"
		"  --peer-host HOST         target broker peer host\n"
		"  --peer-port PORT         target broker peer port (default %u)\n"
		"  --timeout SEC            recv timeout in seconds (default %d)\n"
		"\n"
		"Payload (required):\n"
		"  --trace-id ID            globally unique trace id, e.g. clusterA-100\n"
		"  --src-cluster NAME       originator cluster name\n"
		"  --src-job-id N           originator job id\n"
		"  --src-user USER          originator user name\n"
		"  --remote-user USER       expected target-cluster user name\n"
		"                           (must match receiver's user_mapping)\n"
		"  --partition NAME         target partition name\n"
		"  --app-name NAME          application logical name\n"
		"  --script PATH            originator-side absolute script path\n"
		"  --hop-count N            hop count (default 0; >0 will be rejected)\n"
		"\n"
		"Misc:\n"
		"  -v, --verbose            increase log verbosity (repeatable)\n"
		"  -h, --help               show this help\n",
		prog, DEFAULT_PEER_PORT, DEFAULT_TIMEOUT_S);
}

enum {
	OPT_PEER_HOST = 0x100,
	OPT_PEER_PORT,
	OPT_TIMEOUT,
	OPT_TRACE_ID,
	OPT_SRC_CLUSTER,
	OPT_SRC_JOB_ID,
	OPT_SRC_USER,
	OPT_REMOTE_USER,
	OPT_PARTITION,
	OPT_APP_NAME,
	OPT_SCRIPT,
	OPT_HOP_COUNT,
};

static const struct option long_opts[] = {
	{ "peer-host",   required_argument, NULL, OPT_PEER_HOST   },
	{ "peer-port",   required_argument, NULL, OPT_PEER_PORT   },
	{ "timeout",     required_argument, NULL, OPT_TIMEOUT     },
	{ "trace-id",    required_argument, NULL, OPT_TRACE_ID    },
	{ "src-cluster", required_argument, NULL, OPT_SRC_CLUSTER },
	{ "src-job-id",  required_argument, NULL, OPT_SRC_JOB_ID  },
	{ "src-user",    required_argument, NULL, OPT_SRC_USER    },
	{ "remote-user", required_argument, NULL, OPT_REMOTE_USER },
	{ "partition",   required_argument, NULL, OPT_PARTITION   },
	{ "app-name",    required_argument, NULL, OPT_APP_NAME    },
	{ "script",      required_argument, NULL, OPT_SCRIPT      },
	{ "hop-count",   required_argument, NULL, OPT_HOP_COUNT   },
	{ "verbose",     no_argument,       NULL, 'v' },
	{ "help",        no_argument,       NULL, 'h' },
	{ NULL, 0, NULL, 0 },
};

static int _parse_args(int argc, char **argv, args_t *out)
{
	int c;

	out->peer_port = DEFAULT_PEER_PORT;
	out->timeout_s = DEFAULT_TIMEOUT_S;

	while ((c = getopt_long(argc, argv, "vh", long_opts, NULL)) != -1) {
		switch (c) {
		case OPT_PEER_HOST:
			out->peer_host = optarg;
			break;
		case OPT_PEER_PORT:
			out->peer_port = (uint16_t) atoi(optarg);
			break;
		case OPT_TIMEOUT:
			out->timeout_s = atoi(optarg);
			break;
		case OPT_TRACE_ID:
			out->trace_id = optarg;
			break;
		case OPT_SRC_CLUSTER:
			out->src_cluster = optarg;
			break;
		case OPT_SRC_JOB_ID:
			out->src_job_id = (uint32_t) strtoul(optarg, NULL, 10);
			break;
		case OPT_SRC_USER:
			out->src_user = optarg;
			break;
		case OPT_REMOTE_USER:
			out->remote_user = optarg;
			break;
		case OPT_PARTITION:
			out->partition = optarg;
			break;
		case OPT_APP_NAME:
			out->app_name = optarg;
			break;
		case OPT_SCRIPT:
			out->script = optarg;
			break;
		case OPT_HOP_COUNT:
			out->hop_count = (uint8_t) atoi(optarg);
			break;
		case 'v':
			out->verbose++;
			break;
		case 'h':
			_usage(argv[0]);
			exit(0);
		default:
			_usage(argv[0]);
			return -1;
		}
	}

	if (!out->peer_host || !out->trace_id || !out->src_cluster ||
	    !out->src_user  || !out->remote_user || !out->partition ||
	    !out->app_name  || !out->script || !out->src_job_id) {
		fprintf(stderr,
			"%s: missing required option (run with --help)\n",
			argv[0]);
		return -1;
	}
	if (!out->peer_port) {
		fprintf(stderr, "%s: invalid --peer-port\n", argv[0]);
		return -1;
	}
	if (out->timeout_s <= 0)
		out->timeout_s = DEFAULT_TIMEOUT_S;

	return 0;
}

static void _setup_logging(int verbose)
{
	log_options_t logopt = (log_options_t) LOG_OPTS_STDERR_ONLY;
	int level = LOG_LEVEL_INFO + verbose;

	if (level >= LOG_LEVEL_END)
		level = LOG_LEVEL_END - 1;
	logopt.stderr_level = level;

	if (log_init("inject_broker_forward", logopt,
		     SYSLOG_FACILITY_USER, NULL))
		fprintf(stderr, "log_init failed\n");
}

/*
 * One-shot synchronous request/response. Returns SLURM_SUCCESS on a clean
 * round-trip even if the receiver replied with a non-zero error_code; the
 * caller distinguishes via the printed BROKER_ACK line.
 */
static int _send_and_recv(const args_t *args)
{
	brokerd_broker_forward_job_msg_t req = {
		.trace_id         = args->trace_id,
		.hop_count        = args->hop_count,
		.src_cluster      = args->src_cluster,
		.src_job_id       = args->src_job_id,
		.src_user_name    = args->src_user,
		.remote_user_name = args->remote_user,
		.target_partition = args->partition,
		.app_name         = args->app_name,
		.script_path      = args->script,
	};

	buf_t       *send_buf = NULL;
	buf_t       *recv_buf = NULL;
	char        *raw      = NULL;
	size_t       raw_len  = 0;
	slurm_addr_t addr     = { 0 };
	int          fd       = -1;
	ssize_t      sent;
	uint16_t     got_msg_type = 0;
	uint16_t     got_pv       = 0;
	void        *payload      = NULL;
	int          rc           = SLURM_ERROR;

	send_buf = init_buf(BUF_SIZE);
	if (!send_buf) {
		error("init_buf failed");
		goto out;
	}
	if (brokerd_wire_build(send_buf,
			       BROKERD_REQUEST_BROKER_FORWARD_JOB,
			       &req, SLURM_PROTOCOL_VERSION)) {
		error("brokerd_wire_build failed");
		goto out;
	}
	debug("built BROKER_FORWARD_JOB frame (%u bytes)",
	      (unsigned int) get_buf_offset(send_buf));

	slurm_set_addr(&addr, args->peer_port, args->peer_host);
	fd = slurm_open_msg_conn(&addr);
	if (fd < 0) {
		error("connect to %s:%u failed: %m",
		      args->peer_host, args->peer_port);
		goto out;
	}

	sent = slurm_msg_sendto(fd, get_buf_data(send_buf),
				get_buf_offset(send_buf));
	if (sent < 0) {
		error("slurm_msg_sendto to %s:%u failed: %m",
		      args->peer_host, args->peer_port);
		goto out;
	}
	debug("sent %zd bytes, awaiting BROKER_ACK", sent);

	if (slurm_msg_recvfrom_timeout(fd, &raw, &raw_len,
				       args->timeout_s * 1000) < 0) {
		error("slurm_msg_recvfrom_timeout from %s:%u failed: %m",
		      args->peer_host, args->peer_port);
		goto out;
	}

	recv_buf = create_buf(raw, raw_len);
	raw = NULL; /* ownership transferred to recv_buf */
	if (!recv_buf) {
		error("create_buf failed");
		goto out;
	}

	if (brokerd_wire_parse(recv_buf, &got_msg_type, &got_pv, &payload)) {
		error("brokerd_wire_parse failed");
		goto out;
	}

	if (got_msg_type != BROKERD_RESPONSE_BROKER_ACK) {
		error("unexpected response msg_type=%u (expected %u)",
		      got_msg_type, BROKERD_RESPONSE_BROKER_ACK);
		brokerd_free_msg_data(got_msg_type, payload);
		goto out;
	}

	{
		brokerd_broker_ack_msg_t *ack = payload;

		printf("BROKER_ACK error_code=%u (%s) trace_id=%s "
		       "dst_work_dir=%s\n",
		       ack->error_code,
		       brokerd_strerror((int) ack->error_code),
		       ack->trace_id ? ack->trace_id : "",
		       ack->dst_work_dir ? ack->dst_work_dir : "");
		rc = (ack->error_code == 0) ? SLURM_SUCCESS : SLURM_ERROR;
		brokerd_free_msg_data(got_msg_type, payload);
	}

out:
	if (fd >= 0)
		(void) close(fd);
	FREE_NULL_BUFFER(send_buf);
	FREE_NULL_BUFFER(recv_buf);
	xfree(raw);
	return rc;
}

int main(int argc, char **argv)
{
	args_t args = { 0 };
	int rc;

	if (_parse_args(argc, argv, &args))
		return 2;

	_setup_logging(args.verbose);

	/*
	 * slurm_init(NULL) loads the auth plugin (auth/munge by default)
	 * which brokerd_wire_build needs for auth_g_create / auth_g_pack.
	 * No broker.conf is read; this tool is intentionally stateless.
	 */
	slurm_init(NULL);

	rc = _send_and_recv(&args);

	log_fini();
	return (rc == SLURM_SUCCESS) ? 0 : 1;
}
