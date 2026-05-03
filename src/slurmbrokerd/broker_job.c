/*****************************************************************************\
 *  broker_job.c - slurmbrokerd in-memory job table + JSON serialization.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See broker_job.h for the
 *  contract and doc/checklists/M03-data-persist.md for the rationale.
 *
 *  Slurm-version independence
 *  --------------------------
 *  This module deliberately avoids any slurm-version-bound type. The
 *  earlier `job_desc_msg_t *job_desc` field on broker_job_t (and its
 *  base64-via-pack_msg JSON encoding) was removed because pack_msg's
 *  REQUEST_SUBMIT_BATCH_JOB body changes shape across slurm major
 *  versions, which would break A=24.05 <-> B=23.11 cross-cluster
 *  scenarios. The receiver broker now reconstructs the local job
 *  description on its own libslurm version using the small set of
 *  flat fields (script_path / app_name / remote_user_name /
 *  target_partition / dst_work_dir) shipped on the wire.
\*****************************************************************************/

#include "config.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "slurm/slurm_errno.h"

#include "src/common/data.h"
#include "src/common/list.h"
#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/xhash.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/interfaces/serializer.h"

#include "broker_job.h"

/*****************************************************************************\
 *                              Global table state
\*****************************************************************************/

xhash_t        *g_broker_jobs;
list_t         *g_broker_jobs_list;
pthread_mutex_t g_broker_jobs_lock = PTHREAD_MUTEX_INITIALIZER;

/*****************************************************************************\
 *                         JSON building helpers
\*****************************************************************************/

/* Append a JSON-quoted, escape-safe representation of `s` to `buf`.
 * NULL input is rendered as the JSON literal `null`. */
static void _json_append_str(char **buf, const char *s)
{
	const char *p;

	if (!s) {
		xstrcat(*buf, "null");
		return;
	}
	xstrcatchar(*buf, '"');
	for (p = s; *p; p++) {
		switch (*p) {
		case '"':  xstrcat(*buf, "\\\""); break;
		case '\\': xstrcat(*buf, "\\\\"); break;
		case '\b': xstrcat(*buf, "\\b");  break;
		case '\f': xstrcat(*buf, "\\f");  break;
		case '\n': xstrcat(*buf, "\\n");  break;
		case '\r': xstrcat(*buf, "\\r");  break;
		case '\t': xstrcat(*buf, "\\t");  break;
		default:
			if ((unsigned char) *p < 0x20)
				xstrfmtcat(*buf, "\\u%04x",
					   (unsigned char) *p);
			else
				xstrcatchar(*buf, *p);
		}
	}
	xstrcatchar(*buf, '"');
}

/*****************************************************************************\
 *                          create / destroy
\*****************************************************************************/

broker_job_t *broker_job_create(void)
{
	broker_job_t *job = xmalloc(sizeof(*job));

	slurm_mutex_init(&job->lock);
	job->state            = BROKER_STATE_INIT;
	job->state_enter_time = time(NULL);
	job->role             = BROKER_ROLE_ORIGINATOR;
	return job;
}

void broker_job_destroy(broker_job_t *job)
{
	if (!job)
		return;

	xfree(job->src_user_name);
	xfree(job->remote_user_name);
	xfree(job->account);
	xfree(job->src_cluster);
	xfree(job->dst_cluster);
	xfree(job->target_partition);
	xfree(job->src_work_dir);
	xfree(job->dst_work_dir);
	xfree(job->script_path);
	xfree(job->app_name);
	xfree(job->state_reason);
	xfree(job->remote_alloc_tres);

	slurm_mutex_destroy(&job->lock);
	xfree(job);
}

void broker_job_set_state(broker_job_t *job, broker_job_state_t new_state,
			  const char *reason)
{
	if (!job)
		return;

	slurm_mutex_lock(&job->lock);
	job->state            = new_state;
	job->state_enter_time = time(NULL);
	xfree(job->state_reason);
	if (reason && *reason)
		job->state_reason = xstrdup(reason);
	slurm_mutex_unlock(&job->lock);
}

/*****************************************************************************\
 *                         table CRUD
\*****************************************************************************/

static void _job_idfunc(void *item, const char **key, uint32_t *len)
{
	broker_job_t *j = item;

	*key = j->trace_id;
	*len = strlen(j->trace_id);
}

static void _job_freefunc(void *item)
{
	broker_job_destroy(item);
}

int broker_job_table_init(void)
{
	slurm_mutex_lock(&g_broker_jobs_lock);
	if (g_broker_jobs) {
		slurm_mutex_unlock(&g_broker_jobs_lock);
		return SLURM_SUCCESS;
	}
	g_broker_jobs      = xhash_init(_job_idfunc, _job_freefunc);
	g_broker_jobs_list = list_create(NULL);
	slurm_mutex_unlock(&g_broker_jobs_lock);

	if (!g_broker_jobs || !g_broker_jobs_list) {
		error("%s: allocation failed", __func__);
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

void broker_job_table_fini(void)
{
	slurm_mutex_lock(&g_broker_jobs_lock);
	/* Drop the list reference first so we never touch a job that the
	 * xhash freefunc just released. */
	if (g_broker_jobs_list) {
		list_destroy(g_broker_jobs_list);
		g_broker_jobs_list = NULL;
	}
	if (g_broker_jobs) {
		xhash_free(g_broker_jobs);
		g_broker_jobs = NULL;
	}
	slurm_mutex_unlock(&g_broker_jobs_lock);
}

int broker_job_table_add(broker_job_t *job)
{
	int rc = SLURM_SUCCESS;

	if (!job || !job->trace_id[0])
		return SLURM_ERROR;

	slurm_mutex_lock(&g_broker_jobs_lock);
	if (!g_broker_jobs) {
		rc = SLURM_ERROR;
	} else if (xhash_get_str(g_broker_jobs, job->trace_id)) {
		rc = SLURM_ERROR;
	} else {
		xhash_add(g_broker_jobs, job);
		list_append(g_broker_jobs_list, job);
	}
	slurm_mutex_unlock(&g_broker_jobs_lock);
	return rc;
}

broker_job_t *broker_job_table_get(const char *trace_id)
{
	broker_job_t *j;

	if (!trace_id || !*trace_id)
		return NULL;

	slurm_mutex_lock(&g_broker_jobs_lock);
	j = g_broker_jobs ? xhash_get_str(g_broker_jobs, trace_id) : NULL;
	slurm_mutex_unlock(&g_broker_jobs_lock);
	return j;
}

int broker_job_table_remove(const char *trace_id)
{
	broker_job_t *j;

	if (!trace_id || !*trace_id)
		return SLURM_ERROR;

	slurm_mutex_lock(&g_broker_jobs_lock);
	j = g_broker_jobs ? xhash_get_str(g_broker_jobs, trace_id) : NULL;
	if (j) {
		list_delete_ptr(g_broker_jobs_list, j);
		xhash_delete_str(g_broker_jobs, trace_id);
	}
	slurm_mutex_unlock(&g_broker_jobs_lock);
	return j ? SLURM_SUCCESS : SLURM_ERROR;
}

uint32_t broker_job_table_count(void)
{
	uint32_t n;

	slurm_mutex_lock(&g_broker_jobs_lock);
	n = g_broker_jobs ? xhash_count(g_broker_jobs) : 0;
	slurm_mutex_unlock(&g_broker_jobs_lock);
	return n;
}

void broker_job_table_foreach(int (*fn)(broker_job_t *, void *), void *arg)
{
	list_itr_t *itr;
	broker_job_t *j;

	if (!fn)
		return;

	slurm_mutex_lock(&g_broker_jobs_lock);
	if (!g_broker_jobs_list) {
		slurm_mutex_unlock(&g_broker_jobs_lock);
		return;
	}
	itr = list_iterator_create(g_broker_jobs_list);
	while ((j = list_next(itr)))
		if (fn(j, arg))
			break;
	list_iterator_destroy(itr);
	slurm_mutex_unlock(&g_broker_jobs_lock);
}

/*****************************************************************************\
 *                         to_json
\*****************************************************************************/

char *broker_job_to_json(broker_job_t *job)
{
	char *out = NULL;

	if (!job)
		return NULL;

	xstrcat(out, "{");

	xstrcat(out, "\"trace_id\":");
	_json_append_str(&out, job->trace_id);
	xstrfmtcat(out, ",\"src_job_id\":%u",    job->src_job_id);
	xstrfmtcat(out, ",\"remote_job_id\":%u", job->remote_job_id);
	xstrfmtcat(out, ",\"role\":%d",          (int) job->role);
	xstrfmtcat(out, ",\"hop_count\":%u",     job->hop_count);

	xstrcat(out, ",\"src_user_name\":");
	_json_append_str(&out, job->src_user_name);
	xstrfmtcat(out, ",\"src_uid\":%u", job->src_uid);

	xstrcat(out, ",\"remote_user_name\":");
	_json_append_str(&out, job->remote_user_name);
	xstrfmtcat(out, ",\"remote_uid\":%u", job->remote_uid);
	xstrfmtcat(out, ",\"remote_gid\":%u", job->remote_gid);

	xstrcat(out, ",\"account\":");
	_json_append_str(&out, job->account);

	xstrcat(out, ",\"src_cluster\":");
	_json_append_str(&out, job->src_cluster);
	xstrcat(out, ",\"dst_cluster\":");
	_json_append_str(&out, job->dst_cluster);
	xstrcat(out, ",\"target_partition\":");
	_json_append_str(&out, job->target_partition);

	xstrcat(out, ",\"src_work_dir\":");
	_json_append_str(&out, job->src_work_dir);
	xstrcat(out, ",\"dst_work_dir\":");
	_json_append_str(&out, job->dst_work_dir);
	xstrcat(out, ",\"script_path\":");
	_json_append_str(&out, job->script_path);
	xstrcat(out, ",\"app_name\":");
	_json_append_str(&out, job->app_name);

	xstrfmtcat(out, ",\"state\":%d", (int) job->state);
	xstrcat(out, ",\"state_reason\":");
	_json_append_str(&out, job->state_reason);
	xstrfmtcat(out, ",\"retry_count\":%u",      job->retry_count);
	xstrfmtcat(out, ",\"state_enter_time\":%lld",
		   (long long) job->state_enter_time);
	xstrfmtcat(out, ",\"submit_time\":%lld",
		   (long long) job->submit_time);
	xstrfmtcat(out, ",\"last_poll_time\":%lld",
		   (long long) job->last_poll_time);

	xstrfmtcat(out, ",\"remote_start_time\":%lld",
		   (long long) job->remote_start_time);
	xstrfmtcat(out, ",\"remote_end_time\":%lld",
		   (long long) job->remote_end_time);
	xstrcat(out, ",\"remote_alloc_tres\":");
	_json_append_str(&out, job->remote_alloc_tres);
	xstrfmtcat(out, ",\"remote_exit_code\":%d", job->remote_exit_code);

	xstrfmtcat(out, ",\"cancel_requested\":%s",
		   job->cancel_requested ? "true" : "false");
	xstrfmtcat(out, ",\"cancel_propagated\":%s",
		   job->cancel_propagated ? "true" : "false");

	xstrcat(out, "}");
	return out;
}

/*****************************************************************************\
 *                         from_json
 *
 * Uses Slurm's serializer plugin (loaded by persist_thread_start before any
 * restore is attempted) to parse the JSON line into a data_t tree, then
 * walks the dict to populate broker_job_t. This intentionally tolerates
 * missing optional fields by leaving them at their broker_job_create()
 * defaults; only trace_id is required (everything else is best-effort
 * forward/backward compatible).
 *
 * Note: legacy "job_desc_b64" fields written by older broker versions
 * are silently ignored, since broker_job_t no longer carries a
 * slurm-version-bound job_desc.
\*****************************************************************************/

static const char *_dict_get_str(const data_t *dict, const char *key)
{
	const data_t *d = data_key_get_const(dict, key);

	if (!d)
		return NULL;
	if (data_get_type(d) != DATA_TYPE_STRING)
		return NULL;
	return data_get_string_const(d);
}

static bool _dict_get_int(data_t *dict, const char *key, int64_t *out)
{
	data_t *d = data_key_get(dict, key);

	if (!d)
		return false;
	if (data_get_int_converted(d, out))
		return false;
	return true;
}

static bool _dict_get_bool(data_t *dict, const char *key, bool *out)
{
	data_t *d = data_key_get(dict, key);

	if (!d)
		return false;
	if (data_get_bool_converted(d, out))
		return false;
	return true;
}

static void _dict_take_str(data_t *dict, const char *key, char **dst)
{
	const char *v = _dict_get_str(dict, key);

	if (!v)
		return;
	xfree(*dst);
	*dst = xstrdup(v);
}

#define DICT_GET_U(dict, k, dst, type)                              \
	do {                                                        \
		int64_t _v;                                         \
		if (_dict_get_int((dict), (k), &_v))                \
			(dst) = (type) _v;                          \
	} while (0)

broker_job_t *broker_job_from_json(const char *line)
{
	data_t *root = NULL;
	const char *trace_id;
	broker_job_t *job = NULL;
	int64_t v64;

	if (!line || !*line)
		return NULL;

	if (serialize_g_string_to_data(&root, line, strlen(line),
				       MIME_TYPE_JSON)) {
		error("%s: JSON parse failed", __func__);
		return NULL;
	}
	if (!root || data_get_type(root) != DATA_TYPE_DICT) {
		error("%s: JSON root is not a dict", __func__);
		FREE_NULL_DATA(root);
		return NULL;
	}

	trace_id = _dict_get_str(root, "trace_id");
	if (!trace_id || !*trace_id) {
		error("%s: missing trace_id", __func__);
		goto fail;
	}

	job = broker_job_create();
	/* snprintf is used in place of strlcpy because Slurm only ships
	 * strlcpy.h as a fallback when HAVE_STRLCPY is undefined. snprintf
	 * always NUL-terminates, which is the only behaviour we need here. */
	(void) snprintf(job->trace_id, sizeof(job->trace_id), "%s", trace_id);

	DICT_GET_U(root, "src_job_id",    job->src_job_id,    uint32_t);
	DICT_GET_U(root, "remote_job_id", job->remote_job_id, uint32_t);
	DICT_GET_U(root, "role",          job->role,          broker_role_t);
	DICT_GET_U(root, "hop_count",     job->hop_count,     uint8_t);

	_dict_take_str(root, "src_user_name",    &job->src_user_name);
	DICT_GET_U  (root, "src_uid",          job->src_uid,    uint32_t);
	_dict_take_str(root, "remote_user_name", &job->remote_user_name);
	DICT_GET_U  (root, "remote_uid",       job->remote_uid, uint32_t);
	DICT_GET_U  (root, "remote_gid",       job->remote_gid, uint32_t);
	_dict_take_str(root, "account",          &job->account);

	_dict_take_str(root, "src_cluster",      &job->src_cluster);
	_dict_take_str(root, "dst_cluster",      &job->dst_cluster);
	_dict_take_str(root, "target_partition", &job->target_partition);

	_dict_take_str(root, "src_work_dir", &job->src_work_dir);
	_dict_take_str(root, "dst_work_dir", &job->dst_work_dir);
	_dict_take_str(root, "script_path",  &job->script_path);
	_dict_take_str(root, "app_name",     &job->app_name);

	if (_dict_get_int(root, "state", &v64))
		job->state = (broker_job_state_t) v64;
	_dict_take_str(root, "state_reason", &job->state_reason);
	DICT_GET_U(root, "retry_count", job->retry_count, uint32_t);

	if (_dict_get_int(root, "state_enter_time", &v64))
		job->state_enter_time = (time_t) v64;
	if (_dict_get_int(root, "submit_time", &v64))
		job->submit_time = (time_t) v64;
	if (_dict_get_int(root, "last_poll_time", &v64))
		job->last_poll_time = (time_t) v64;
	if (_dict_get_int(root, "remote_start_time", &v64))
		job->remote_start_time = (time_t) v64;
	if (_dict_get_int(root, "remote_end_time", &v64))
		job->remote_end_time = (time_t) v64;

	_dict_take_str(root, "remote_alloc_tres", &job->remote_alloc_tres);
	if (_dict_get_int(root, "remote_exit_code", &v64))
		job->remote_exit_code = (int32_t) v64;

	_dict_get_bool(root, "cancel_requested",  &job->cancel_requested);
	_dict_get_bool(root, "cancel_propagated", &job->cancel_propagated);

	FREE_NULL_DATA(root);
	return job;

fail:
	FREE_NULL_DATA(root);
	if (job)
		broker_job_destroy(job);
	return NULL;
}
