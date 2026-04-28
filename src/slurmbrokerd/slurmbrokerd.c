/*****************************************************************************\
 *  slurmbrokerd.c - Cross-domain Slurm broker daemon, main entry.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP).
 *  See doc/Broker详细设计文档MVP.md  for design.
 *  See doc/Broker开发任务清单.md     for module / task breakdown.
 *
 *  M01-T1 : skeleton only - compiles, links, returns 0.
 *  M01-T2~T7 will fill in CLI parsing, logging, signals, main loop,
 *  init/fini orchestration and systemd integration.
\*****************************************************************************/

#include <stddef.h>

#include "slurmbrokerd.h"

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

int broker_init(void)
{
	/* TODO M02-T3: broker_conf_init(g_argv_opts.conf_path); */
	/* TODO M02-T4: user_mapping_load(...);                  */
	/* TODO M03-T6: broker_state_restore();                  */
	/* TODO M04-T2: proto_init();                            */
	/* TODO M08-T1: egress_init();                           */
	/* TODO M09-T1: state_machine_start();                   */
	/* TODO M10-T1: stage_pool_start();                      */
	/* TODO M13-T1: sync_ticker_start();                     */
	/* TODO M05-T1: listener_start();                        */
	return 0;
}

void broker_fini(void)
{
	/* TODO: tear down sub-modules in reverse order. */
}

int main(int argc, char **argv)
{
	(void) argc;
	(void) argv;

	/* TODO M01-T2: _parse_commandline(argc, argv);    */
	/* TODO M01-T3: _setup_logging();                  */
	/* TODO M01-T4: _install_signal_handlers();        */
	/* TODO M01-T5: main loop until g_shutdown_requested */
	/* TODO M01-T6: broker_init() / broker_fini()      */

	return 0;
}
