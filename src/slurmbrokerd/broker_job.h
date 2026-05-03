/*****************************************************************************\
 *  broker_job.h - slurmbrokerd in-memory job table & JSON serialization.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/Broker详细设计文档MVP.md
 *  §4 and doc/checklists/M03-data-persist.md for design.
 *
 *  This header is the **single source of truth** for the broker job state
 *  enum, the `broker_job_t` struct, and the global job table API. All other
 *  modules (M07/M08/M09/M13) MUST include this header rather than redefining.
\*****************************************************************************/

#ifndef _BROKERD_BROKER_JOB_H
#define _BROKERD_BROKER_JOB_H

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>

/*
 * Intentionally NOT including <slurm/slurm.h> here. broker_job_t carries
 * only a handful of broker-flat fields; cross-cluster forwarding never
 * traffics in slurm-version-bound types like job_desc_msg_t. Receivers
 * reconstruct the local job description from (script_path, app_name,
 * remote_user_name, target_partition, dst_work_dir) using their own
 * libslurm version's slurm_init_job_desc_msg() + slurm_submit_batch_job_msg().
 */

#include "src/common/list.h"
#include "src/common/xhash.h"

/*
 * trace_id format: "<src_cluster>-<src_job_id>". 48 bytes is sized to fit
 * a 32-char cluster name plus a uint32 decimal plus '-' plus terminator
 * with comfortable headroom.
 */
#define BROKER_TRACE_ID_LEN 48

typedef enum {
	BROKER_STATE_INIT          = 0, /* in table, waiting STAGING_IN     */
	BROKER_STATE_STAGING_IN    = 1, /* rsync src -> remote dst_work_dir */
	BROKER_STATE_STAGED_IN     = 2, /* very short-lived; mostly memory  */
	BROKER_STATE_SUBMITTED     = 3, /* remote sbatch'd; waiting RUNNING */
	BROKER_STATE_RUNNING       = 4, /* remote squeue says RUNNING       */
	BROKER_STATE_STAGING_OUT   = 5, /* remote done; rsync back to src   */
	BROKER_STATE_COMPLETED     = 6, /* terminal: success                */
	BROKER_STATE_FAILED        = 7, /* terminal: failure                */
	BROKER_STATE_CANCELLED     = 8, /* terminal: scancel'd              */
} broker_job_state_t;

typedef enum {
	BROKER_ROLE_ORIGINATOR = 0,
	BROKER_ROLE_RECEIVER   = 1,
} broker_role_t;

/*
 * Field order is locked by §4.1 of the design doc and reflected in the
 * JSON schema below. Do NOT reorder; add new fields at the end before
 * the lock so on-disk JSONL stays forward-compatible.
 */
typedef struct broker_job {
	/* identity */
	char       trace_id[BROKER_TRACE_ID_LEN];
	uint32_t   src_job_id;
	uint32_t   remote_job_id;          /* 0 == not yet remote-submitted */
	broker_role_t role;
	uint8_t    hop_count;

	/* user identity */
	char      *src_user_name;
	uint32_t   src_uid;
	char      *remote_user_name;
	uint32_t   remote_uid;
	uint32_t   remote_gid;
	char      *account;

	/* cluster routing */
	char      *src_cluster;
	char      *dst_cluster;
	char      *target_partition;

	/* working directories */
	char      *src_work_dir;
	char      *dst_work_dir;
	char      *script_path;       /* originator-side absolute path; the
				       * remote broker only uses basename()
				       * after rsync has placed the file under
				       * dst_work_dir. */

	/* application identity (drives lookup_software.sh + script rewrite).
	 * This deliberately replaces the earlier job_desc_msg_t* field so
	 * broker_job_t is free of any slurm-version-bound types. */
	char      *app_name;

	/* state machine */
	broker_job_state_t state;
	char      *state_reason;
	uint32_t   retry_count;
	time_t     state_enter_time;
	time_t     submit_time;
	time_t     last_poll_time;

	/* terminal accounting */
	time_t     remote_start_time;
	time_t     remote_end_time;
	char      *remote_alloc_tres;
	int32_t    remote_exit_code;

	/* cancel propagation (MVP) */
	bool       cancel_requested;
	bool       cancel_propagated;

	/* per-job lock (state machine vs. async callbacks) */
	pthread_mutex_t lock;
} broker_job_t;

/*
 * Global table. Owns each broker_job_t exactly once (xhash holds the free
 * function; the list parallels it for ordered iteration but does NOT free).
 *
 * g_broker_jobs_lock guards both the xhash and the list. It does NOT cover
 * mutation of fields inside an individual broker_job_t; for those, take
 * job->lock after dropping g_broker_jobs_lock to keep the read path cheap.
 */
extern xhash_t        *g_broker_jobs;
extern list_t         *g_broker_jobs_list;
extern pthread_mutex_t g_broker_jobs_lock;

/* lifecycle */
extern int  broker_job_table_init(void);
extern void broker_job_table_fini(void);

/* per-job */
extern broker_job_t *broker_job_create(void);
extern void          broker_job_destroy(broker_job_t *job);

/* table CRUD (all internally locked) */
extern int          broker_job_table_add(broker_job_t *job);
extern broker_job_t *broker_job_table_get(const char *trace_id);
extern int          broker_job_table_remove(const char *trace_id);
extern uint32_t     broker_job_table_count(void);

/*
 * Iterate the table while holding g_broker_jobs_lock. The callback MUST
 * NOT call back into any broker_job_table_* function (would self-deadlock)
 * and MUST NOT block on slow I/O. Return non-zero from the callback to
 * stop iteration early.
 */
extern void broker_job_table_foreach(int (*fn)(broker_job_t *, void *),
				     void *arg);

/*
 * JSON serialization. to_json returns an xmalloc'd line (no trailing '\n');
 * from_json takes a single line (with or without trailing '\n') and returns
 * a fresh broker_job_t (caller must broker_job_table_add or destroy it).
 */
extern char         *broker_job_to_json(broker_job_t *job);
extern broker_job_t *broker_job_from_json(const char *line);

#endif /* _BROKERD_BROKER_JOB_H */
