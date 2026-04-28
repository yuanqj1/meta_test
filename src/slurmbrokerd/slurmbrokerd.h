/*****************************************************************************\
 *  slurmbrokerd.h - Cross-domain Slurm broker daemon, public types & globals.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/Broker详细设计文档MVP.md
 *  and doc/Broker开发任务清单.md for design and task breakdown.
\*****************************************************************************/

#ifndef _SLURMBROKERD_H
#define _SLURMBROKERD_H

#include <signal.h>
#include <stdbool.h>

/*
 * Set by signal handlers; read by the main loop.
 * Defined in slurmbrokerd.c.
 */
extern volatile sig_atomic_t g_shutdown_requested;
extern volatile sig_atomic_t g_sighup_requested;

/*
 * Parsed command line options. Populated in _parse_commandline()
 * (see M01-T2). Defined in slurmbrokerd.c.
 */
typedef struct {
	bool foreground;	/* -D : run in foreground, log to stderr */
	char *conf_path;	/* -f : path to broker.conf */
	int verbose;		/* -v count : raise log verbosity */
} broker_argv_opts_t;

extern broker_argv_opts_t g_argv_opts;

/*
 * Lifecycle hooks. Implemented in slurmbrokerd.c.
 *
 * broker_init()  is the single place that wires up every sub-module
 *                in the right order (M01-T6 will fill in real calls
 *                to M02..M13 init functions).
 * broker_fini()  is the symmetric tear-down.
 *
 * Returns SLURM_SUCCESS on success, SLURM_ERROR on failure.
 */
extern int broker_init(void);
extern void broker_fini(void);

#endif /* _SLURMBROKERD_H */
