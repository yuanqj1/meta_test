/*****************************************************************************\
 *  persist.c - JSONL three-file atomic checkpoint + 30s background tick.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See persist.h for the
 *  contract and doc/checklists/M03-data-persist.md for the rationale.
 *
 *  Layout on disk (all under broker_conf.state_save_location):
 *      <state_file_name>           current checkpoint
 *      <state_file_name>.tmp       in-flight write, atomically renamed
 *      <state_file_name>.old       previous checkpoint, crash fallback
\*****************************************************************************/

#include "config.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "slurm/slurm.h"

#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/interfaces/serializer.h"

#include "broker_conf.h"
#include "broker_job.h"
#include "persist.h"

/*****************************************************************************\
 *                       module-local state
\*****************************************************************************/

static pthread_t       persist_tid;
static pthread_cond_t  persist_cond  = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t persist_mutex = PTHREAD_MUTEX_INITIALIZER;
static bool            persist_running       = false;
static bool            persist_thread_active = false;
static bool            persist_async_pending = false;

/*
 * Restore line buffer is heap-allocated so we never put a 64KB array on
 * the broker's main-thread stack (large array jobs can balloon job_desc
 * and therefore one JSONL row).
 */
#define PERSIST_LINE_BUF_SZ (256 * 1024)

/*****************************************************************************\
 *                       path helpers
\*****************************************************************************/

static char *_path_current(void)
{
	char *p = NULL;

	xstrfmtcat(p, "%s/%s",
		   g_broker_conf.state_save_location,
		   g_broker_conf.state_file_name);
	return p;
}

static char *_path_tmp(const char *cur)
{
	char *p = NULL;

	xstrfmtcat(p, "%s.tmp", cur);
	return p;
}

static char *_path_old(const char *cur)
{
	char *p = NULL;

	xstrfmtcat(p, "%s.old", cur);
	return p;
}

/*****************************************************************************\
 *                       broker_state_save
\*****************************************************************************/

typedef struct {
	FILE *fp;
	int   error;
	uint32_t written;
} _save_ctx_t;

/*
 * Per-job foreach callback. Runs while g_broker_jobs_lock is held inside
 * broker_job_table_foreach(). We therefore do only in-memory work here:
 * build the JSON string and write it via stdio (which buffers in the same
 * thread). I/O fsync happens after the foreach returns, outside the lock.
 */
static int _save_one(broker_job_t *job, void *arg)
{
	_save_ctx_t *ctx = arg;
	char *json;

	if (ctx->error)
		return 1;

	json = broker_job_to_json(job);
	if (!json) {
		error("%s: to_json returned NULL for trace_id=%s",
		      __func__, job->trace_id);
		ctx->error = 1;
		return 1;
	}
	if (fprintf(ctx->fp, "%s\n", json) < 0) {
		error("%s: fprintf failed: %m", __func__);
		ctx->error = 1;
	} else {
		ctx->written++;
	}
	xfree(json);
	return ctx->error;
}

int broker_state_save(void)
{
	char *path_cur = NULL, *path_tmp = NULL, *path_old = NULL;
	_save_ctx_t ctx = { 0 };
	int rc = SLURM_ERROR;

	if (!g_broker_conf.state_save_location ||
	    !g_broker_conf.state_file_name) {
		error("%s: state file path not configured", __func__);
		return SLURM_ERROR;
	}

	path_cur = _path_current();
	path_tmp = _path_tmp(path_cur);
	path_old = _path_old(path_cur);

	ctx.fp = fopen(path_tmp, "w");
	if (!ctx.fp) {
		error("%s: open %s: %m", __func__, path_tmp);
		goto out;
	}

	broker_job_table_foreach(_save_one, &ctx);

	if (ctx.error) {
		(void) fclose(ctx.fp);
		(void) unlink(path_tmp);
		goto out;
	}

	if (fflush(ctx.fp) || fsync(fileno(ctx.fp))) {
		error("%s: flush/fsync %s: %m", __func__, path_tmp);
		(void) fclose(ctx.fp);
		(void) unlink(path_tmp);
		goto out;
	}
	if (fclose(ctx.fp)) {
		error("%s: close %s: %m", __func__, path_tmp);
		(void) unlink(path_tmp);
		goto out;
	}

	/*
	 * current -> .old ; missing on first ever run is OK.
	 * tmp     -> current ; this is the atomic step that consumers see.
	 */
	if (rename(path_cur, path_old) && errno != ENOENT) {
		error("%s: rename %s -> %s: %m",
		      __func__, path_cur, path_old);
		(void) unlink(path_tmp);
		goto out;
	}
	if (rename(path_tmp, path_cur)) {
		error("%s: rename %s -> %s: %m",
		      __func__, path_tmp, path_cur);
		goto out;
	}

	debug("broker_state_save: %u jobs persisted to %s",
	      ctx.written, path_cur);
	rc = SLURM_SUCCESS;

out:
	xfree(path_cur);
	xfree(path_tmp);
	xfree(path_old);
	return rc;
}

/*****************************************************************************\
 *                       broker_state_restore
\*****************************************************************************/

static int _restore_from(const char *path, uint32_t *out_loaded,
			 uint32_t *out_skipped)
{
	FILE *fp;
	char *line;
	uint32_t loaded = 0, skipped = 0;
	uint32_t line_no = 0;

	fp = fopen(path, "r");
	if (!fp) {
		if (errno == ENOENT)
			return -1;
		error("%s: open %s: %m", __func__, path);
		return -1;
	}

	line = xmalloc(PERSIST_LINE_BUF_SZ);
	while (fgets(line, PERSIST_LINE_BUF_SZ, fp)) {
		broker_job_t *j;
		size_t len;

		line_no++;
		len = strlen(line);
		if (len > 0 && line[len - 1] == '\n')
			line[len - 1] = '\0';
		if (line[0] == '\0')
			continue;

		j = broker_job_from_json(line);
		if (!j) {
			warning("restore: skipping malformed line %u in %s",
			        line_no, path);
			skipped++;
			continue;
		}
		if (broker_job_table_add(j) != SLURM_SUCCESS) {
			warning("restore: duplicate trace_id=%s on line %u, dropping",
			        j->trace_id, line_no);
			broker_job_destroy(j);
			skipped++;
			continue;
		}
		loaded++;
	}
	xfree(line);
	(void) fclose(fp);

	info("restore: %s -> loaded=%u skipped=%u",
	     path, loaded, skipped);
	if (out_loaded)
		*out_loaded = loaded;
	if (out_skipped)
		*out_skipped = skipped;
	return 0;
}

int broker_state_restore(void)
{
	char *cur = NULL, *old = NULL;
	struct stat st;
	uint32_t loaded = 0, skipped = 0;
	int rc;

	if (!g_broker_conf.state_save_location ||
	    !g_broker_conf.state_file_name) {
		info("restore: state file path not configured, starting empty");
		return SLURM_SUCCESS;
	}

	cur = _path_current();
	old = _path_old(cur);

	if (stat(cur, &st) == 0 && st.st_size > 0) {
		rc = _restore_from(cur, &loaded, &skipped);
	} else {
		if (errno != ENOENT && stat(cur, &st) != 0)
			warning("restore: stat %s: %m", cur);
		else
			info("restore: primary state %s missing/empty, falling back to .old",
			     cur);
		rc = _restore_from(old, &loaded, &skipped);
		if (rc < 0) {
			info("restore: no usable state file, starting with 0 jobs");
			rc = 0;
		}
	}

	xfree(cur);
	xfree(old);
	return (rc < 0) ? SLURM_ERROR : SLURM_SUCCESS;
}

/*****************************************************************************\
 *                       persist_async_request
\*****************************************************************************/

void persist_async_request(void)
{
	slurm_mutex_lock(&persist_mutex);
	persist_async_pending = true;
	slurm_cond_signal(&persist_cond);
	slurm_mutex_unlock(&persist_mutex);
}

/*****************************************************************************\
 *                       background checkpoint thread
\*****************************************************************************/

static void *_persist_main(void *arg)
{
	(void) arg;

	while (1) {
		struct timespec ts;
		uint32_t timeout = g_broker_conf.checkpoint_interval;
		bool stop_now = false;

		if (!timeout)
			timeout = 30;

		clock_gettime(CLOCK_REALTIME, &ts);
		ts.tv_sec += timeout;

		slurm_mutex_lock(&persist_mutex);
		while (persist_running && !persist_async_pending) {
			int err = pthread_cond_timedwait(&persist_cond,
							 &persist_mutex,
							 &ts);
			if (err == ETIMEDOUT)
				break;
			if (err && err != EINTR) {
				errno = err;
				error("%s: cond_timedwait: %m", __func__);
				break;
			}
		}
		persist_async_pending = false;
		stop_now = !persist_running;
		slurm_mutex_unlock(&persist_mutex);

		if (stop_now)
			break;

		if (broker_state_save() != SLURM_SUCCESS)
			error("persist: periodic checkpoint failed");
	}

	/*
	 * One last save before we exit so anything mutated between the
	 * previous checkpoint and SIGTERM is durable.
	 */
	if (broker_state_save() != SLURM_SUCCESS)
		error("persist: final checkpoint failed");

	return NULL;
}

/*****************************************************************************\
 *                       lifecycle
\*****************************************************************************/

int persist_thread_start(void)
{
	if (persist_thread_active) {
		debug("%s: already started", __func__);
		return SLURM_SUCCESS;
	}

	/*
	 * The serializer plugin must be live before any from_json call.
	 * broker_state_restore() runs before persist_thread_start() so we
	 * also call this in broker_init() before restore; calling it twice
	 * here is a safe no-op (serializer_g_init is internally idempotent
	 * for an already-loaded plugin set).
	 */
	if (serializer_g_init(MIME_TYPE_JSON_PLUGIN, NULL)) {
		error("%s: serializer_g_init(json) failed", __func__);
		return SLURM_ERROR;
	}

	slurm_mutex_lock(&persist_mutex);
	persist_running = true;
	persist_async_pending = false;
	slurm_mutex_unlock(&persist_mutex);

	slurm_thread_create(&persist_tid, _persist_main, NULL);
	persist_thread_active = true;
	info("persist: checkpoint thread started (interval=%us)",
	     g_broker_conf.checkpoint_interval);
	return SLURM_SUCCESS;
}

void persist_thread_stop(void)
{
	if (!persist_thread_active)
		return;

	slurm_mutex_lock(&persist_mutex);
	persist_running = false;
	slurm_cond_signal(&persist_cond);
	slurm_mutex_unlock(&persist_mutex);

	(void) pthread_join(persist_tid, NULL);
	persist_thread_active = false;
	info("persist: checkpoint thread stopped");
}
