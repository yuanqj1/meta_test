/*****************************************************************************\
 *  broker_conf.h - slurmbrokerd configuration loader.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP).
\*****************************************************************************/

#ifndef _BROKER_CONF_H
#define _BROKER_CONF_H

#include <stdint.h>

typedef struct {
	/* === cluster identity === */
	char *cluster_name;
	char *broker_node_name;

	/* === service listeners === */
	uint16_t ctld_port;
	uint16_t peer_port;

	/* === single peer (MVP) === */
	char *remote_cluster_name;
	char *remote_broker_host;
	uint16_t remote_broker_port;
	char *remote_munge_key_path;

	/* === target partition === */
	char *default_remote_partition;

	/* === auth === */
	char *auth_type;

	/* === persistence === */
	char *state_save_location;
	char *state_file_name;
	uint32_t checkpoint_interval;

	/* === throttling === */
	uint32_t max_inflight;
	uint64_t max_stage_bytes;

	/* === polling === */
	uint32_t poll_interval;
	uint32_t poll_max_retries;

	/* === data transfer === */
	char *stage_rsync_bin;
	char *stage_ssh_key;
	char *stage_ssh_user;
	uint16_t stage_worker_count;
	uint32_t stage_timeout_per_gb;

	/* === software lookup === */
	char *lookup_software_script;
	uint32_t lookup_timeout_sec;

	/* === retention === */
	uint32_t remote_work_dir_retention_hours;
	uint32_t remote_work_dir_failure_retention_days;

	/* === ABI reserve === */
	void *reserved[8];
} broker_conf_t;

extern broker_conf_t g_broker_conf;

extern int broker_conf_init(const char *path);
extern void broker_conf_log_summary(void);
extern void broker_conf_fini(void);

#endif /* _BROKER_CONF_H */
