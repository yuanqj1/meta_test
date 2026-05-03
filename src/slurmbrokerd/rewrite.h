/*****************************************************************************\
 *  rewrite.h - RECEIVER-side sbatch script rewrite (M12).
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/checklists/M12-rewrite.md
 *  and doc/Broker详细设计文档MVP.md §8.2.2.
 *
 *  Purpose
 *  -------
 *  Just before the receiver broker calls `sudo -u <remote_user> sbatch
 *  ...` (M07 handle_broker_staged_in -> _sudo_sbatch), this module
 *  produces a "remote-friendly" copy of the originator's script:
 *
 *    1. Strip site-specific SBATCH directives the originator embedded
 *       but the remote cluster cannot honour:
 *         - `#SBATCH --reservation=<X>`     (originator's reservation)
 *         - `#SBATCH --cross-domain`        (broker-only marker)
 *         - `#SBATCH --app=<X>`             (broker-only marker)
 *         - `#SBATCH --account=<X>` / `-A`  (let remote sacctmgr pick
 *                                             the user's default account)
 *
 *    2. Override the partition to the broker-configured remote one:
 *         - `#SBATCH -p <X>` / `--partition=<X>`  ->  target_partition
 *
 *    3. Substitute software install paths so `source /xxx/setup.sh`
 *       lines that reference the originator cluster's install root
 *       point at the equivalent path on the remote cluster:
 *         - lookup_software_script <src_cluster> <app_name> -> src_root
 *         - lookup_software_script <dst_cluster> <app_name> -> dst_root
 *         - replace every occurrence of src_root with dst_root
 *
 *  Output
 *  ------
 *  Writes <dst_work_dir>/<basename(script_path)>.cd_modified.sh with
 *  mode 0700. The original <basename(script_path)> file is left
 *  untouched on disk so debugging is easy.
 *
 *  Slurm-version independence
 *  --------------------------
 *  No libslurm calls; pure file IO + fork/exec lookup_software_script.
 *
 *  Tolerance
 *  ---------
 *  When `lookup_software_script` is unset or fails (script missing /
 *  app unknown), this module falls back to "SBATCH-only rewrite" --
 *  the source-line path substitution is skipped, but the modified
 *  script is still produced and submission can proceed. This matches
 *  the design doc's "best-effort" semantics for software lookup.
\*****************************************************************************/

#ifndef _BROKERD_REWRITE_H
#define _BROKERD_REWRITE_H

#include "broker_job.h"

/*
 * Rewrite the staged script for `job` and produce a sibling
 * "<basename>.cd_modified.sh" file under job->dst_work_dir.
 *
 * On success:
 *   *out_modified_path = xstrdup'd absolute path to the new file
 *                        (caller must xfree)
 *   The file's permission is 0700, owned by the broker process uid
 *   (NOT remote_user_name; the remote sbatch sudo'd to remote_user
 *   will read it via group/world bits which we do NOT grant -- so the
 *   sbatch must run as a uid that can read the broker-owned file, OR
 *   the deployment must give /var/lib/slurm-broker (or wherever) g+rx
 *   permissions; M15 sudoers / fs setup covers this).
 *
 * Returns:
 *   SLURM_SUCCESS         - rewrite produced the file (with or without
 *                           path substitution; see "Tolerance" above).
 *   SLURM_ERROR           - filesystem-level failure (cannot read
 *                           original script, cannot write modified
 *                           script, etc); a half-written file is
 *                           cleaned up before return.
 *
 * The function never crashes on unusual input (binary script files,
 * non-UTF-8 content, etc) -- worst case the resulting file is
 * byte-for-byte identical to the input minus the SBATCH header
 * pruning.
 */
extern int rewrite_job_script(broker_job_t *job, char **out_modified_path);

#endif /* _BROKERD_REWRITE_H */
