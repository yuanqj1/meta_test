/*****************************************************************************\
 *  software.h - resolve <cluster, app> to an absolute install path.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/checklists/M11-software.md
 *  and doc/Broker详细设计文档MVP.md §8.2.1.
 *
 *  Purpose
 *  -------
 *  M12 rewrite needs to translate "source /xian/opt/apps/gromacs-X" lines
 *  in the user's submitted script into "source /wz/opt/apps/gromacs-X"
 *  before sbatch runs on the receiver cluster. To do that the broker
 *  needs to know the install root for app `gromacs` on each cluster.
 *
 *  We delegate the actual lookup to an operator-managed shell script
 *  (typically /opt/slurm-broker/scripts/lookup_software.sh; full path is
 *  set in broker.conf::LookupSoftwareScript). The script protocol is:
 *
 *      argv[1] = <cluster>
 *      argv[2] = <app>
 *      stdout  = first line is the absolute path
 *      exit 0  = success; non-zero = unknown / failure
 *
 *  This module wraps the fork+exec+pipe+timeout dance and validates the
 *  output (must be a non-empty `/`-prefixed path). Any failure causes
 *  M12 to skip the path-prefix substitution gracefully (SBATCH-only
 *  rewrite) so a partial deployment never blocks job submission.
 *
 *  Slurm-version independence
 *  --------------------------
 *  No libslurm calls; only POSIX pipe/fork/execl/select/waitpid plus
 *  Slurm-internal xmalloc/log/xstring helpers (version-stable).
\*****************************************************************************/

#ifndef _BROKERD_SOFTWARE_H
#define _BROKERD_SOFTWARE_H

/*
 * Synchronous lookup. Blocks the calling thread until the script
 * finishes or the configured timeout (broker.conf::LookupTimeoutSec,
 * default 10s) elapses.
 *
 * IN  cluster   - non-empty cluster name (e.g. "wz_cluster")
 * IN  app       - non-empty app name (e.g. "gromacs")
 * OUT out_path  - on SLURM_SUCCESS, set to xstrdup'd absolute path;
 *                 caller xfree()s. Untouched on any failure.
 *
 * Returns:
 *   SLURM_SUCCESS              - *out_path is a valid absolute path
 *   BROKERD_ERR_LOOKUP_TIMEOUT - script ran past LookupTimeoutSec
 *   BROKERD_ERR_LOOKUP_FAILED  - script not configured / non-zero exit /
 *                                empty output / non-`/`-prefixed path /
 *                                fork or pipe error
 */
extern int lookup_software_path(const char *cluster, const char *app,
				char **out_path);

#endif /* _BROKERD_SOFTWARE_H */
