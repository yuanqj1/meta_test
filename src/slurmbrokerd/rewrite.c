/*****************************************************************************\
 *  rewrite.c - RECEIVER-side sbatch script rewrite (M12).
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See rewrite.h for the
 *  contract and doc/checklists/M12-rewrite.md for the design.
 *
 *  Pipeline
 *  --------
 *    1. _read_file(<dst_work_dir>/<basename>) -> orig_text
 *    2. Per-line scan:
 *         - drop lines matching #SBATCH --reservation / --cross-domain
 *           / --app / --account / -A
 *         - rewrite #SBATCH -p / --partition= to use target_partition
 *         - keep everything else verbatim
 *    3. If lookup succeeded for both clusters, run global string
 *       substitution src_root -> dst_root over the assembled text.
 *    4. _write_file(<dst_work_dir>/<basename>.cd_modified.sh, mode 0700).
 *
 *  Slurm-version independence
 *  --------------------------
 *  Only POSIX file IO + fork/exec. No libslurm RPCs.
\*****************************************************************************/

#include "config.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "broker_conf.h"
#include "broker_job.h"
#include "rewrite.h"

#define LOOKUP_TIMEOUT_DEFAULT_S  10
#define MODIFIED_SUFFIX           ".cd_modified.sh"

/*****************************************************************************\
 *                       waitpid + timeout (private copy)
 *
 * Same shape as handler_remote.c / stage.c; duplicated rather than
 * abstracted to keep modules independent. If a third caller ever
 * appears we'll factor this into a small helper module.
\*****************************************************************************/

static int _waitpid_timeout(pid_t pid, int timeout_s)
{
	int wstat = 0;
	int slept_us = 0;
	const int step_us = 100 * 1000;
	const int budget_us = timeout_s * 1000 * 1000;

	while (slept_us < budget_us) {
		pid_t r = waitpid(pid, &wstat, WNOHANG);

		if (r == pid)
			break;
		if (r < 0 && errno != EINTR) {
			error("rewrite: waitpid: %m");
			return -1;
		}
		usleep(step_us);
		slept_us += step_us;
	}

	if (slept_us >= budget_us) {
		warning("rewrite: child pid=%d exceeded %ds timeout, sending SIGKILL",
		        (int) pid, timeout_s);
		(void) kill(pid, SIGKILL);
		(void) waitpid(pid, &wstat, 0);
		return -1;
	}
	if (!WIFEXITED(wstat))
		return -1;
	return WEXITSTATUS(wstat);
}

/*****************************************************************************\
 *                       _lookup_software_path
 *
 * Run "<lookup_software_script> <cluster> <app>", capture the first
 * line of stdout (newline-trimmed) into *out (xstrdup'd).
 *
 * Returns:
 *   SLURM_SUCCESS  - *out points at the resolved path
 *   SLURM_ERROR    - script unset / fork failed / non-zero exit / no
 *                    parseable output. Caller treats this as "unknown,
 *                    skip path substitution".
\*****************************************************************************/

static int _lookup_software_path(const char *cluster, const char *app,
				 char **out)
{
	int pipefd[2] = { -1, -1 };
	pid_t pid;
	char buf[1024];
	size_t off = 0;
	int rc, timeout_s;
	char *p;

	*out = NULL;

	if (!cluster || !*cluster || !app || !*app) {
		debug("rewrite: lookup skipped (cluster='%s' app='%s')",
		      cluster ? cluster : "(null)",
		      app ? app : "(null)");
		return SLURM_ERROR;
	}
	if (!g_broker_conf.lookup_software_script ||
	    !*g_broker_conf.lookup_software_script) {
		debug("rewrite: lookup_software_script not configured, skip");
		return SLURM_ERROR;
	}

	if (pipe(pipefd) < 0) {
		error("rewrite: pipe: %m");
		return SLURM_ERROR;
	}

	pid = fork();
	if (pid < 0) {
		error("rewrite: fork: %m");
		(void) close(pipefd[0]);
		(void) close(pipefd[1]);
		return SLURM_ERROR;
	}
	if (pid == 0) {
		(void) close(pipefd[0]);
		if (pipefd[1] != STDOUT_FILENO) {
			(void) dup2(pipefd[1], STDOUT_FILENO);
			(void) close(pipefd[1]);
		}
		/* leave stderr attached to the broker journal so lookup
		 * script error messages reach the operator */
		execl(g_broker_conf.lookup_software_script,
		      g_broker_conf.lookup_software_script,
		      cluster, app, (char *) NULL);
		_exit(127);
	}

	(void) close(pipefd[1]);
	memset(buf, 0, sizeof(buf));
	while (off + 1 < sizeof(buf)) {
		ssize_t n = read(pipefd[0], buf + off, sizeof(buf) - 1 - off);

		if (n > 0)
			off += (size_t) n;
		else if (n == 0)
			break;
		else if (errno == EINTR)
			continue;
		else
			break;
	}
	buf[off] = '\0';
	(void) close(pipefd[0]);

	timeout_s = (int) g_broker_conf.lookup_timeout_sec;
	if (timeout_s <= 0)
		timeout_s = LOOKUP_TIMEOUT_DEFAULT_S;

	rc = _waitpid_timeout(pid, timeout_s);
	if (rc != 0) {
		debug("rewrite: lookup_software(%s, %s) rc=%d",
		      cluster, app, rc);
		return SLURM_ERROR;
	}

	/* Trim trailing whitespace / newline; consume only the first
	 * line. The script convention is one absolute path per line,
	 * primary line first. */
	p = strpbrk(buf, "\r\n");
	if (p)
		*p = '\0';
	for (p = buf + strlen(buf); p > buf; p--) {
		if (!isspace((unsigned char) p[-1]))
			break;
		p[-1] = '\0';
	}
	if (!buf[0]) {
		debug("rewrite: lookup_software(%s, %s) returned empty",
		      cluster, app);
		return SLURM_ERROR;
	}

	*out = xstrdup(buf);
	debug("rewrite: lookup_software(%s, %s) -> %s",
	      cluster, app, *out);
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       per-line SBATCH helpers
\*****************************************************************************/

/* Locate the start of an SBATCH directive on a #SBATCH line, or NULL
 * if the line is not an SBATCH directive at all. The returned pointer
 * is into the input buffer (no copy). Examples:
 *   "#SBATCH -p foo"           -> "-p foo"
 *   "#  SBATCH --nodes=4"      -> "--nodes=4"
 *   "echo SBATCH hello"        -> NULL (not a comment)
 */
static const char *_sbatch_directive(const char *line)
{
	const char *p = line;

	if (*p != '#')
		return NULL;
	p++;
	while (*p == ' ' || *p == '\t')
		p++;
	if (strncmp(p, "SBATCH", 6))
		return NULL;
	p += 6;
	while (*p == ' ' || *p == '\t')
		p++;
	return p;
}

/* True if `arg_start` (after "#SBATCH ") begins with one of the
 * options we drop entirely. Looks for both "--opt=val" and "--opt val"
 * forms, plus the short "-A" alias for --account. */
static bool _sbatch_arg_should_drop(const char *arg_start)
{
	static const char *long_drops[] = {
		"--reservation", "--cross-domain", "--app", "--account",
		NULL,
	};

	for (int i = 0; long_drops[i]; i++) {
		size_t l = strlen(long_drops[i]);

		if (!strncmp(arg_start, long_drops[i], l) &&
		    (arg_start[l] == '\0' || arg_start[l] == '=' ||
		     arg_start[l] == ' ' || arg_start[l] == '\t' ||
		     arg_start[l] == '\n'))
			return true;
	}
	/* short form -A */
	if (!strncmp(arg_start, "-A", 2) &&
	    (arg_start[2] == ' ' || arg_start[2] == '\t' ||
	     arg_start[2] == '=' || arg_start[2] == '\0' ||
	     arg_start[2] == '\n'))
		return true;
	return false;
}

static bool _line_should_drop(const char *line)
{
	const char *arg = _sbatch_directive(line);

	if (!arg)
		return false;
	return _sbatch_arg_should_drop(arg);
}

/*
 * If `line` is an SBATCH partition line, return an xmalloc'd rewritten
 * copy with `new_part` substituted. Otherwise NULL (caller keeps the
 * original line). Handles both forms:
 *   "#SBATCH --partition=foo[trailing]"
 *   "#SBATCH -p foo[trailing]"
 *
 * `trailing` (anything after the value, including comments / extra args
 * / the newline) is preserved verbatim.
 */
static char *_line_replace_partition(const char *line, const char *new_part)
{
	const char *arg = _sbatch_directive(line);
	const char *value_start;
	const char *value_end;
	const char *prefix_end;
	char *out = NULL;

	if (!arg || !new_part || !*new_part)
		return NULL;

	if (!strncmp(arg, "--partition=", 12)) {
		value_start = arg + 12;
		prefix_end  = value_start; /* keep "...--partition=" */
	} else if (!strncmp(arg, "--partition", 11) &&
		   (arg[11] == ' ' || arg[11] == '\t')) {
		value_start = arg + 11;
		while (*value_start == ' ' || *value_start == '\t')
			value_start++;
		prefix_end  = value_start;
	} else if (!strncmp(arg, "-p", 2) &&
		   (arg[2] == ' ' || arg[2] == '\t' || arg[2] == '=')) {
		value_start = arg + 2;
		while (*value_start == ' ' || *value_start == '\t' ||
		       *value_start == '=')
			value_start++;
		prefix_end  = value_start;
	} else {
		return NULL;
	}

	value_end = value_start;
	while (*value_end && *value_end != ' ' && *value_end != '\t' &&
	       *value_end != '\n')
		value_end++;

	xstrfmtcat(out, "%.*s%s%s",
		   (int) (prefix_end - line), line, new_part, value_end);
	return out;
}

/*****************************************************************************\
 *                       global string substitution
\*****************************************************************************/

static char *_substitute_all(const char *text, const char *find,
			     const char *replace)
{
	char *out = NULL;
	const char *cur = text;
	const char *p;
	size_t flen;

	if (!text)
		return NULL;
	if (!find || !*find || !strstr(text, find))
		return xstrdup(text);

	flen = strlen(find);
	while ((p = strstr(cur, find))) {
		if (p > cur)
			xstrncat(out, cur, (size_t) (p - cur));
		if (replace && *replace)
			xstrcat(out, replace);
		cur = p + flen;
	}
	xstrcat(out, cur);
	return out ? out : xstrdup("");
}

/*****************************************************************************\
 *                       file IO helpers
\*****************************************************************************/

/* Read the entire file at `path` into an xmalloc'd nul-terminated
 * buffer. Returns SLURM_SUCCESS / SLURM_ERROR. */
static int _read_file(const char *path, char **out, size_t *out_len)
{
	FILE *fp;
	char *buf = NULL;
	size_t cap = 0, len = 0;
	const size_t chunk = 8192;

	*out = NULL;
	if (out_len)
		*out_len = 0;

	fp = fopen(path, "r");
	if (!fp) {
		error("rewrite: open %s: %m", path);
		return SLURM_ERROR;
	}

	for (;;) {
		size_t n;

		if (cap - len < chunk) {
			cap = cap ? cap * 2 : chunk * 2;
			buf = xrealloc(buf, cap);
		}
		n = fread(buf + len, 1, cap - len - 1, fp);
		len += n;
		if (n == 0)
			break;
	}
	if (ferror(fp)) {
		error("rewrite: read %s: %m", path);
		(void) fclose(fp);
		xfree(buf);
		return SLURM_ERROR;
	}
	(void) fclose(fp);

	if (!buf) {
		buf = xmalloc(1);
		buf[0] = '\0';
	} else {
		buf[len] = '\0';
	}
	*out = buf;
	if (out_len)
		*out_len = len;
	return SLURM_SUCCESS;
}

/* Write `data` (length-prefixed; not necessarily nul-terminated) to
 * `path` with mode 0700. Truncates if exists. Atomicity is not
 * required: we tolerate a torn write because the caller will refuse
 * to submit if rewrite returns SLURM_ERROR, and the receiver-side
 * cleanup (M14) eventually purges the dst_work_dir. */
static int _write_file(const char *path, const char *data, size_t len)
{
	int fd;
	ssize_t n;
	size_t off = 0;

	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0700);
	if (fd < 0) {
		error("rewrite: open(write) %s: %m", path);
		return SLURM_ERROR;
	}
	while (off < len) {
		n = write(fd, data + off, len - off);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			error("rewrite: write %s: %m", path);
			(void) close(fd);
			(void) unlink(path);
			return SLURM_ERROR;
		}
		off += (size_t) n;
	}
	if (close(fd) < 0) {
		error("rewrite: close %s: %m", path);
		(void) unlink(path);
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       _do_rewrite (per-line + full-text passes)
\*****************************************************************************/

/* Append `s` (NUL-terminated) to `*pp`, growing as needed via xstrcat. */
static void _append(char **pp, const char *s)
{
	if (s && *s)
		xstrcat(*pp, s);
}

/*
 * Walk `orig` line-by-line, applying drop / partition-replace rules.
 * Returns an xmalloc'd buffer with the per-line transformations
 * applied; full-text path substitution happens in the caller.
 */
static char *_apply_per_line_rules(const char *orig, const char *target_part)
{
	char *out = NULL;
	const char *line_start = orig;

	while (*line_start) {
		const char *line_end = strchr(line_start, '\n');
		size_t line_len;
		char *line_buf;
		char *replaced;

		if (line_end)
			line_len = (size_t) (line_end - line_start) + 1;
		else
			line_len = strlen(line_start);

		line_buf = xmalloc(line_len + 1);
		memcpy(line_buf, line_start, line_len);
		line_buf[line_len] = '\0';

		if (_line_should_drop(line_buf)) {
			debug2("rewrite: drop line: %s", line_buf);
			xfree(line_buf);
			line_start += line_len;
			continue;
		}

		replaced = _line_replace_partition(line_buf, target_part);
		if (replaced) {
			debug2("rewrite: partition: %s -> %s",
			       line_buf, replaced);
			_append(&out, replaced);
			xfree(replaced);
		} else {
			_append(&out, line_buf);
		}
		xfree(line_buf);
		line_start += line_len;
	}
	if (!out)
		out = xstrdup("");
	return out;
}

/*
 * Build "<dst_work_dir>/<basename(script_path)>" (xmalloc'd).
 * Returns NULL if either field is missing. Uses the same basename
 * convention as handler_remote.c::_path_basename.
 */
static const char *_basename_of(const char *path)
{
	const char *p;

	if (!path || !*path)
		return "";
	p = strrchr(path, '/');
	return p ? p + 1 : path;
}

/*****************************************************************************\
 *                       rewrite_job_script (public)
\*****************************************************************************/

int rewrite_job_script(broker_job_t *job, char **out_modified_path)
{
	char *src_root = NULL, *dst_root = NULL;
	char *orig_path = NULL, *modified_path = NULL;
	char *orig_text = NULL, *post_line = NULL, *final_text = NULL;
	const char *base;
	int rc = SLURM_ERROR;

	if (out_modified_path)
		*out_modified_path = NULL;

	if (!job || !job->dst_work_dir || !job->script_path ||
	    !job->target_partition) {
		error("rewrite: missing required fields on job %s",
		      job ? job->trace_id : "(null)");
		return SLURM_ERROR;
	}

	base = _basename_of(job->script_path);
	if (!*base) {
		error("rewrite: empty basename for trace_id=%s",
		      job->trace_id);
		return SLURM_ERROR;
	}
	xstrfmtcat(orig_path, "%s/%s", job->dst_work_dir, base);
	xstrfmtcat(modified_path, "%s%s", orig_path, MODIFIED_SUFFIX);

	/* 1. Read original (the rsync stage-in must have placed it here). */
	if (_read_file(orig_path, &orig_text, NULL) != SLURM_SUCCESS) {
		error("rewrite: trace_id=%s cannot read %s",
		      job->trace_id, orig_path);
		goto out;
	}

	/* 2. Per-line SBATCH rules (drop + partition rewrite). Always done. */
	post_line = _apply_per_line_rules(orig_text,
					  job->target_partition);

	/* 3. Best-effort path substitution. lookup failure is non-fatal. */
	(void) _lookup_software_path(job->src_cluster, job->app_name,
				     &src_root);
	(void) _lookup_software_path(job->dst_cluster, job->app_name,
				     &dst_root);

	if (src_root && dst_root && xstrcmp(src_root, dst_root)) {
		final_text = _substitute_all(post_line, src_root, dst_root);
		debug("rewrite: trace_id=%s path subst '%s' -> '%s'",
		      job->trace_id, src_root, dst_root);
	} else {
		if (!src_root || !dst_root) {
			debug("rewrite: trace_id=%s skipping path subst (src=%s dst=%s)",
			      job->trace_id,
			      src_root ? src_root : "(null)",
			      dst_root ? dst_root : "(null)");
		}
		final_text = xstrdup(post_line);
	}

	/* 4. Write modified script. */
	if (_write_file(modified_path, final_text,
			strlen(final_text)) != SLURM_SUCCESS) {
		error("rewrite: trace_id=%s cannot write %s",
		      job->trace_id, modified_path);
		goto out;
	}

	info("rewrite: trace_id=%s -> %s", job->trace_id, modified_path);
	if (out_modified_path) {
		*out_modified_path = modified_path;
		modified_path = NULL; /* ownership transferred */
	}
	rc = SLURM_SUCCESS;

out:
	xfree(orig_path);
	xfree(modified_path);
	xfree(orig_text);
	xfree(post_line);
	xfree(final_text);
	xfree(src_root);
	xfree(dst_root);
	return rc;
}
