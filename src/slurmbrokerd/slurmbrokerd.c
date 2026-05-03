/*****************************************************************************\
 *  slurmbrokerd.c - Cross-domain Slurm broker daemon, main entry.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP).
 *  See doc/Broker详细设计文档MVP.md  for design.
 *  See doc/Broker开发任务清单.md     for module / task breakdown.
 *
 *  M01-T1 : skeleton only - compiles, links, returns 0.
 *  M01-T2 : command line parsing (-D / -f / -v / -h / -V).
 *  M01-T3 : logging via log_init() with verbose level mapping.
 *  M01-T4 : signal handling (SIGTERM/SIGINT/SIGHUP/SIGPIPE).
 *  M01-T5 : main loop polls g_shutdown_requested with sleep(1).
 *  M01-T6 : broker_init() / broker_fini() central orchestration.
 *  M01-T7 : systemd unit (etc/slurmbrokerd.service.in) is installed
 *           via etc/Makefile.am's WITH_SYSTEMD_UNITS path - no work
 *           is required in this file.
\*****************************************************************************/

#include "config.h"

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "slurm/slurm.h"

#include "src/common/daemonize.h"
#include "src/common/log.h"
#include "src/common/proc_args.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/interfaces/serializer.h"

#include "broker_conf.h"
#include "broker_job.h"
#include "listener.h"
#include "persist.h"
#include "proto.h"
#include "slurmbrokerd.h"
#include "user_mapping.h"

#define DEFAULT_BROKER_CONF_PATH "/etc/slurm/broker.conf"
#define DEFAULT_BROKER_PIDFILE "/run/slurmbrokerd/slurmbrokerd.pid"

/*
 * Globals declared in slurmbrokerd.h. Defined here exactly once.
 */
volatile sig_atomic_t g_shutdown_requested = 0;
volatile sig_atomic_t g_sighup_requested = 0;
broker_argv_opts_t g_argv_opts = {
	.foreground = false,
	.conf_path = NULL,
	.verbose = 0,
};

/* Tracks whether slurm_init() has actually been invoked, so that
 * broker_fini() can safely skip slurm_fini() on early init failures. */
static bool g_slurm_inited = false;

static void _usage(void)
{
	fprintf(stderr,
		"Usage: slurmbrokerd [OPTIONS]\n"
		"\n"
		"Slurm cross-domain broker daemon (MVP).\n"
		"\n"
		"Options:\n"
		"  -D                Run in foreground; log to stderr.\n"
		"  -f <path>         Path to broker.conf (default: %s).\n"
		"  -v                Increase verbosity (repeat: -vv, -vvv).\n"
		"  -h, --help        Show this help and exit.\n"
		"  -V, --version     Show version and exit.\n"
		"\n"
		"Signals:\n"
		"  SIGTERM, SIGINT   Graceful shutdown.\n"
		"  SIGHUP            Reload user mapping (TODO M02-T4).\n",
		DEFAULT_BROKER_CONF_PATH);
}

static void _parse_commandline(int argc, char **argv)
{
	static const struct option long_options[] = {
		{ "help",    no_argument, NULL, 'h' },
		{ "version", no_argument, NULL, 'V' },
		{ NULL,      0,           NULL, 0   },
	};
	int c;
	int option_index = 0;

	if (!g_argv_opts.conf_path)
		g_argv_opts.conf_path = xstrdup(DEFAULT_BROKER_CONF_PATH);

	/*
	 * Suppress getopt's own diagnostics so that all error reporting
	 * is funneled through _usage() for a uniform UX.
	 */
	opterr = 0;

	while ((c = getopt_long(argc, argv, "Df:hVv",
				long_options, &option_index)) != -1) {
		switch (c) {
		case 'D':
			g_argv_opts.foreground = true;
			break;
		case 'f':
			xfree(g_argv_opts.conf_path);
			g_argv_opts.conf_path = xstrdup(optarg);
			break;
		case 'v':
			g_argv_opts.verbose++;
			break;
		case 'h':
			_usage();
			exit(0);
		case 'V':
			print_slurm_version();
			exit(0);
		default:
			if (optopt)
				fprintf(stderr,
					"slurmbrokerd: invalid option -- '%c'\n",
					optopt);
			_usage();
			exit(1);
		}
	}

	if (optind < argc) {
		fprintf(stderr,
			"slurmbrokerd: unexpected positional argument: '%s'\n",
			argv[optind]);
		_usage();
		exit(1);
	}
}

/*
 * _setup_logging - initialise the Slurm log subsystem according to the
 * command line options parsed in _parse_commandline().
 *
 * Verbose level mapping (matches slurmrestd):
 *   default        -> LOG_LEVEL_INFO
 *   -v             -> LOG_LEVEL_VERBOSE
 *   -vv            -> LOG_LEVEL_DEBUG
 *   -vvv           -> LOG_LEVEL_DEBUG2
 *   -vvvv          -> LOG_LEVEL_DEBUG3
 *   -vvvvv         -> LOG_LEVEL_DEBUG4
 *   -vvvvvv (or more) -> LOG_LEVEL_DEBUG5  (clamped to LOG_LEVEL_END - 1)
 *
 * Foreground mode (-D): log to stderr only, facility USER.
 * Daemon mode        : log to syslog only, facility DAEMON.
 */
static void _setup_logging(int argc, char **argv)
{
	log_options_t logopt;
	log_facility_t fac;
	int level = LOG_LEVEL_INFO + g_argv_opts.verbose;

	if (level >= LOG_LEVEL_END)
		level = LOG_LEVEL_END - 1;

	if (g_argv_opts.foreground) {
		logopt = (log_options_t) LOG_OPTS_STDERR_ONLY;
		logopt.stderr_level = level;
		fac = SYSLOG_FACILITY_USER;
	} else {
		logopt = (log_options_t) LOG_OPTS_INITIALIZER;
		logopt.stderr_level = LOG_LEVEL_QUIET;
		logopt.logfile_level = LOG_LEVEL_QUIET;
		logopt.syslog_level = level;
		fac = SYSLOG_FACILITY_DAEMON;
	}

	if (log_init(xbasename(argv[0]), logopt, fac, NULL))
		fatal("Unable to setup logging: %m");
}

/*
 * Async-signal-safe handlers.
 *
 * They MUST NOT call non-async-signal-safe functions (no info()/error()/
 * malloc/free/etc.). They only flip a flag that the main loop polls.
 */
static void _shutdown_handler(int sig)
{
	(void) sig;
	g_shutdown_requested = 1;
}

static void _sighup_handler(int sig)
{
	(void) sig;
	g_sighup_requested = 1;
}

/*
 * _install_signal_handlers - install daemon-wide signal handlers.
 *
 * SIGTERM / SIGINT  -> request graceful shutdown.
 * SIGHUP            -> request user_mapping reload (M02-T4 wires the body).
 * SIGPIPE           -> ignored: we treat short writes as regular EPIPE
 *                      from send()/write() return values instead.
 *
 * SA_RESTART is intentionally NOT set, so blocking syscalls (sleep, poll,
 * accept, recv, ...) return -1/EINTR on signal. The main loop checks
 * g_shutdown_requested right after, which gives us the "<= 5s shutdown"
 * latency required by the M01-T4 DoD.
 */
static void _install_signal_handlers(void)
{
	struct sigaction sa = { 0 };

	if (sigemptyset(&sa.sa_mask))
		fatal("%s: sigemptyset failed: %m", __func__);
	sa.sa_flags = 0;

	sa.sa_handler = _shutdown_handler;
	if (sigaction(SIGTERM, &sa, NULL))
		fatal("%s: sigaction(SIGTERM) failed: %m", __func__);
	if (sigaction(SIGINT, &sa, NULL))
		fatal("%s: sigaction(SIGINT) failed: %m", __func__);

	sa.sa_handler = _sighup_handler;
	if (sigaction(SIGHUP, &sa, NULL))
		fatal("%s: sigaction(SIGHUP) failed: %m", __func__);

	sa.sa_handler = SIG_IGN;
	if (sigaction(SIGPIPE, &sa, NULL))
		fatal("%s: sigaction(SIGPIPE) failed: %m", __func__);
}

/*
 * broker_init - bring up every sub-module in deterministic order.
 *
 * The order mirrors the runtime data flow:
 *   conf -> persistent state -> RPC layer -> egress / state-machine /
 *   stage / sync-ticker -> finally the listener that opens the
 *   front door to ctld and remote brokers.
 *
 * Each sub-module has its own task (M02..M13). Until those land, the
 * body here is intentionally a list of TODOs so the wiring is easy
 * to spot during review.
 *
 * Returns SLURM_SUCCESS on success, SLURM_ERROR on any sub-module
 * init failure. On failure the caller must still invoke broker_fini()
 * to release whatever was already brought up.
 */
int broker_init(void)
{
	if (broker_conf_init(g_argv_opts.conf_path))
		return SLURM_ERROR;
	broker_conf_log_summary();

	/*
	 * Bring up the libslurm subsystems we depend on:
	 *   - slurm_init(NULL) wires conf + auth/cred/tls/hash/etc plugins;
	 *     auth_g_create/pack/unpack inside proto.c require this.
	 *   - serializer_g_init loads the JSON plugin used by
	 *     broker_job_from_json() during state restore.
	 * Both are idempotent if called more than once.
	 */
	slurm_init(NULL);
	g_slurm_inited = true;

	if (serializer_g_init(MIME_TYPE_JSON_PLUGIN, NULL)) {
		error("broker_init: serializer_g_init(json) failed");
		return SLURM_ERROR;
	}

	if (broker_job_table_init() != SLURM_SUCCESS) {
		error("broker_init: broker_job_table_init failed");
		return SLURM_ERROR;
	}
	if (broker_state_restore() != SLURM_SUCCESS) {
		error("broker_init: broker_state_restore failed");
		return SLURM_ERROR;
	}
	if (persist_thread_start() != SLURM_SUCCESS) {
		error("broker_init: persist_thread_start failed");
		return SLURM_ERROR;
	}
	if (proto_init() != SLURM_SUCCESS) {
		error("broker_init: proto_init failed");
		return SLURM_ERROR;
	}

	/*
	 * Listener brings up two listening sockets:
	 *   - g_broker_conf.ctld_port (8442): slurm-native frame for
	 *     ctld <-> broker traffic. Requires the slurmctld engineer's
	 *     sister PR to register the 4 ctld<->broker msg_types in
	 *     src/common/ before real ctld traffic flows. Until that
	 *     lands, slurm_receive_msg() rejects unknown msg_types with
	 *     SLURM_UNEXPECTED_MSG_ERROR; broker stays alive.
	 *   - g_broker_conf.peer_port (8443): broker private wire frame
	 *     for broker <-> broker traffic; independent of any
	 *     slurmctld-side change.
	 *
	 * Started LAST so all upstream sub-modules (broker_job table,
	 * persist thread, proto layer) are ready to service requests the
	 * moment the listener accepts the first connection.
	 */
	if (listener_start() != SLURM_SUCCESS) {
		error("broker_init: listener_start failed");
		return SLURM_ERROR;
	}

	/* TODO M08-T1: egress_init();                                  */
	/* TODO M09-T1: state_machine_start();                          */
	/* TODO M10-T1: stage_pool_start();                             */
	/* TODO M13-T1: sync_ticker_start();                            */

	debug("broker_init: M02/M03/M04/M05 ready (later sub-modules not wired yet)");
	return SLURM_SUCCESS;
}

/*
 * broker_fini - reverse of broker_init.
 *
 * Must be safe to call after a partially failed broker_init(): each
 * sub-module's *_fini() / *_stop() must be a no-op when its *_init()
 * was never called.
 */
void broker_fini(void)
{
	/* TODO M13-T1: sync_ticker_stop();                             */
	/* TODO M10-T1: stage_pool_stop();                              */
	/* TODO M09-T1: state_machine_stop();                           */
	/* TODO M08-T1: egress_fini();                                  */

	/* Stop the listener FIRST so no fresh inbound RPC can land while
	 * downstream sub-modules are tearing down. The listener thread
	 * holds no locks across its select() boundary, so this only
	 * costs <= 1s (the select timeout). */
	listener_stop();

	/* RPC layer drops its peer endpoint; safe after listener stop
	 * because no inbound path can now drive an outbound proto call. */
	proto_fini();

	/* Stop the checkpoint thread first so the worker cannot race a
	 * freed table; the thread also performs one final save during its
	 * own teardown. */
	persist_thread_stop();
	broker_job_table_fini();

	user_mapping_destroy_all();
	broker_conf_fini();

	/* Symmetric to slurm_init(NULL) in broker_init(). Released only
	 * if init actually got that far so an early-failure path through
	 * broker_init -> broker_fini does not poke un-initialised plugin
	 * tables. */
	if (g_slurm_inited) {
		slurm_fini();
		g_slurm_inited = false;
	}

	xfree(g_argv_opts.conf_path);
}

int main(int argc, char **argv)
{
	int pidfd = -1;
	int rc;

	_parse_commandline(argc, argv);
	_setup_logging(argc, argv);
	_install_signal_handlers();

	if (!g_argv_opts.foreground) {
		if (xdaemon())
			fatal("Couldn't daemonize slurmbrokerd: %m");
		pidfd = create_pidfile(DEFAULT_BROKER_PIDFILE, 0);
		if (pidfd < 0)
			fatal("Unable to create pidfile `%s'",
			      DEFAULT_BROKER_PIDFILE);
	}
	test_core_limit();

	info("slurmbrokerd %s starting", SLURM_VERSION_STRING);

	if ((rc = broker_init()) != SLURM_SUCCESS) {
		error("broker_init failed (rc=%d), aborting", rc);
		broker_fini();
		if (pidfd >= 0 && unlink(DEFAULT_BROKER_PIDFILE) < 0)
			error("Unable to remove pidfile `%s': %m",
			      DEFAULT_BROKER_PIDFILE);
		if (pidfd >= 0)
			(void) close(pidfd);
		log_fini();
		return 1;
	}

	/*
	 * MVP main loop: poll g_shutdown_requested with a 1-second tick.
	 *
	 * The listener (M05-T1) and other workers will run in their own
	 * threads, so this thread has nothing to do but wait for a signal.
	 * Because SA_RESTART is not set on our handlers, blocking syscalls
	 * such as poll()/accept()/recv() return with EINTR on signal. The
	 * current sleep(1) loop also wakes early on signal delivery, keeping
	 * shutdown latency below the 5s M01-T4 target.
	 */
	while (!g_shutdown_requested) {
		if (g_sighup_requested) {
			g_sighup_requested = 0;
			info("SIGHUP received: configuration reload is restart-only in MVP");
		}
		sleep(1);
	}

	info("graceful shutdown initiated");

	/*
	 * Force one synchronous flush before we tear anything down so the
	 * very latest state is on disk even if the persist thread is still
	 * mid-sleep when shutdown was requested. broker_fini() will join
	 * the worker which performs its own final flush; this extra one
	 * keeps the shutdown latency tight from the user's point of view.
	 */
	if (broker_state_save() != SLURM_SUCCESS)
		error("shutdown: pre-fini broker_state_save failed");

	if (pidfd >= 0 && unlink(DEFAULT_BROKER_PIDFILE) < 0)
		error("Unable to remove pidfile `%s': %m",
		      DEFAULT_BROKER_PIDFILE);

	broker_fini();

	if (pidfd >= 0)
		(void) close(pidfd);

	info("slurmbrokerd shutdown complete");
	log_fini();
	return 0;
}
