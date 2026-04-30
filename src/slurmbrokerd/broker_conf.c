/*****************************************************************************\
 *  broker_conf.c - slurmbrokerd configuration loader.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP).
\*****************************************************************************/

#include "config.h"

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "slurm/slurm.h"

#include "src/common/fd.h"
#include "src/common/log.h"
#include "src/common/parse_config.h"
#include "src/common/xmalloc.h"

#include "broker_conf.h"
#include "user_mapping.h"

#define DEFAULT_CTLD_PORT 8442
#define DEFAULT_PEER_PORT 8443
#define DEFAULT_AUTH_TYPE "auth/munge"
#define DEFAULT_STATE_FILE_NAME "broker_state.jsonl"
#define DEFAULT_CHECKPOINT_INTERVAL 30
#define DEFAULT_MAX_INFLIGHT 500
#define DEFAULT_MAX_STAGE_BYTES 53687091200ULL
#define DEFAULT_POLL_INTERVAL 10
#define DEFAULT_POLL_MAX_RETRIES 5
#define DEFAULT_STAGE_RSYNC_BIN "/usr/bin/rsync"
#define DEFAULT_STAGE_WORKER_COUNT 4
#define DEFAULT_STAGE_TIMEOUT_PER_GB 120
#define DEFAULT_LOOKUP_TIMEOUT_SEC 3
#define DEFAULT_REMOTE_WORK_DIR_RETENTION_HOURS 24
#define DEFAULT_REMOTE_WORK_DIR_FAILURE_RETENTION_DAYS 7

broker_conf_t g_broker_conf;

static const s_p_options_t mapping_line_options[] = {
	{ "UserMapping", S_P_STRING },
	{ "LocalUser", S_P_STRING },
	{ "RemoteCluster", S_P_STRING },
	{ "RemoteUser", S_P_STRING },
	{ "RemoteUid", S_P_UINT32 },
	{ "RemoteGid", S_P_UINT32 },
	{ NULL }
};

static const s_p_options_t broker_options[] = {
	{ "ClusterName", S_P_STRING },
	{ "BrokerNodeName", S_P_STRING },
	{ "BrokerCtldPort", S_P_UINT16 },
	{ "BrokerPeerPort", S_P_UINT16 },
	{ "RemoteClusterName", S_P_STRING },
	{ "RemoteBrokerHost", S_P_STRING },
	{ "RemoteBrokerPort", S_P_UINT16 },
	{ "RemoteMungeKeyPath", S_P_STRING },
	{ "DefaultRemotePartition", S_P_STRING },
	{ "AuthType", S_P_STRING },
	{ "StateSaveLocation", S_P_STRING },
	{ "StateFileName", S_P_STRING },
	{ "CheckpointInterval", S_P_UINT32 },
	{ "MaxInFlightJobs", S_P_UINT32 },
	{ "MaxStageBytes", S_P_UINT64 },
	{ "PollInterval", S_P_UINT32 },
	{ "PollMaxRetries", S_P_UINT32 },
	{ "StageRsyncBin", S_P_STRING },
	{ "StageSshKey", S_P_STRING },
	{ "StageSshUser", S_P_STRING },
	{ "StageWorkerCount", S_P_UINT16 },
	{ "StageTimeoutPerGB", S_P_UINT32 },
	{ "LookupSoftwareScript", S_P_STRING },
	{ "LookupTimeoutSec", S_P_UINT32 },
	{ "RemoteWorkDirRetentionHours", S_P_UINT32 },
	{ "RemoteWorkDirFailureRetentionDays", S_P_UINT32 },
	{ "UserMapping", S_P_LINE, NULL, NULL, mapping_line_options },
	{ NULL }
};

static void _get_string_required(s_p_hashtbl_t *tbl, const char *key,
				 char **field)
{
	if (!s_p_get_string(field, key, tbl) || !*field || !**field)
		fatal("broker.conf: missing required '%s'", key);
}

static void _get_string_optional(s_p_hashtbl_t *tbl, const char *key,
				 char **field, const char *def)
{
	if (!s_p_get_string(field, key, tbl) && def)
		*field = xstrdup(def);
}

static void _get_uint16_default(s_p_hashtbl_t *tbl, const char *key,
				uint16_t *field, uint16_t def)
{
	if (!s_p_get_uint16(field, key, tbl))
		*field = def;
}

static void _get_uint32_default(s_p_hashtbl_t *tbl, const char *key,
				uint32_t *field, uint32_t def)
{
	if (!s_p_get_uint32(field, key, tbl))
		*field = def;
}

static void _get_uint64_default(s_p_hashtbl_t *tbl, const char *key,
				uint64_t *field, uint64_t def)
{
	if (!s_p_get_uint64(field, key, tbl))
		*field = def;
}

static void _validate_port(const char *key, uint16_t port)
{
	if (port < 1024)
		fatal("broker.conf: %s %u out of range [1024, 65535]",
		      key, port);
}

static void _validate_or_die(void)
{
	struct stat st;

	_validate_port("BrokerCtldPort", g_broker_conf.ctld_port);
	_validate_port("BrokerPeerPort", g_broker_conf.peer_port);
	if (g_broker_conf.ctld_port == g_broker_conf.peer_port)
		fatal("broker.conf: BrokerCtldPort and BrokerPeerPort must differ");

	if (stat(g_broker_conf.state_save_location, &st) < 0) {
		if (errno != ENOENT ||
		    mkdirpath(g_broker_conf.state_save_location, 0755, true))
			fatal("broker.conf: cannot create StateSaveLocation %s: %m",
			      g_broker_conf.state_save_location);
	} else if (!S_ISDIR(st.st_mode)) {
		fatal("broker.conf: StateSaveLocation %s is not a directory",
		      g_broker_conf.state_save_location);
	}
	if (access(g_broker_conf.state_save_location, W_OK) < 0)
		fatal("broker.conf: StateSaveLocation %s is not writable: %m",
		      g_broker_conf.state_save_location);

	if (stat(g_broker_conf.stage_ssh_key, &st) < 0)
		fatal("broker.conf: StageSshKey %s missing: %m",
		      g_broker_conf.stage_ssh_key);
	if (st.st_mode & 077)
		fatal("broker.conf: StageSshKey %s permissions %#o too open (need 0600)",
		      g_broker_conf.stage_ssh_key, st.st_mode & 0777);

	if (access(g_broker_conf.lookup_software_script, X_OK) < 0)
		fatal("broker.conf: LookupSoftwareScript %s not executable: %m",
		      g_broker_conf.lookup_software_script);
}

int broker_conf_init(const char *path)
{
	s_p_hashtbl_t *tbl;

	if (!path)
		fatal("broker.conf: no config path supplied");

	broker_conf_fini();

	tbl = s_p_hashtbl_create(broker_options);
	if (s_p_parse_file(tbl, NULL, (char *)path, 0, NULL) == SLURM_ERROR) {
		s_p_hashtbl_destroy(tbl);
		fatal("broker.conf: parse failed for %s", path);
	}

	_get_string_required(tbl, "ClusterName", &g_broker_conf.cluster_name);
	_get_string_required(tbl, "BrokerNodeName",
			     &g_broker_conf.broker_node_name);
	_get_uint16_default(tbl, "BrokerCtldPort", &g_broker_conf.ctld_port,
			    DEFAULT_CTLD_PORT);
	_get_uint16_default(tbl, "BrokerPeerPort", &g_broker_conf.peer_port,
			    DEFAULT_PEER_PORT);
	_get_string_required(tbl, "RemoteClusterName",
			     &g_broker_conf.remote_cluster_name);
	_get_string_required(tbl, "RemoteBrokerHost",
			     &g_broker_conf.remote_broker_host);
	_get_uint16_default(tbl, "RemoteBrokerPort",
			    &g_broker_conf.remote_broker_port,
			    DEFAULT_PEER_PORT);
	_get_string_optional(tbl, "RemoteMungeKeyPath",
			     &g_broker_conf.remote_munge_key_path, NULL);
	_get_string_required(tbl, "DefaultRemotePartition",
			     &g_broker_conf.default_remote_partition);
	_get_string_optional(tbl, "AuthType", &g_broker_conf.auth_type,
			     DEFAULT_AUTH_TYPE);
	_get_string_required(tbl, "StateSaveLocation",
			     &g_broker_conf.state_save_location);
	_get_string_optional(tbl, "StateFileName", &g_broker_conf.state_file_name,
			     DEFAULT_STATE_FILE_NAME);
	_get_uint32_default(tbl, "CheckpointInterval",
			    &g_broker_conf.checkpoint_interval,
			    DEFAULT_CHECKPOINT_INTERVAL);
	_get_uint32_default(tbl, "MaxInFlightJobs", &g_broker_conf.max_inflight,
			    DEFAULT_MAX_INFLIGHT);
	_get_uint64_default(tbl, "MaxStageBytes",
			    &g_broker_conf.max_stage_bytes,
			    DEFAULT_MAX_STAGE_BYTES);
	_get_uint32_default(tbl, "PollInterval", &g_broker_conf.poll_interval,
			    DEFAULT_POLL_INTERVAL);
	_get_uint32_default(tbl, "PollMaxRetries",
			    &g_broker_conf.poll_max_retries,
			    DEFAULT_POLL_MAX_RETRIES);
	_get_string_optional(tbl, "StageRsyncBin",
			     &g_broker_conf.stage_rsync_bin,
			     DEFAULT_STAGE_RSYNC_BIN);
	_get_string_required(tbl, "StageSshKey", &g_broker_conf.stage_ssh_key);
	_get_string_required(tbl, "StageSshUser", &g_broker_conf.stage_ssh_user);
	_get_uint16_default(tbl, "StageWorkerCount",
			    &g_broker_conf.stage_worker_count,
			    DEFAULT_STAGE_WORKER_COUNT);
	_get_uint32_default(tbl, "StageTimeoutPerGB",
			    &g_broker_conf.stage_timeout_per_gb,
			    DEFAULT_STAGE_TIMEOUT_PER_GB);
	_get_string_required(tbl, "LookupSoftwareScript",
			     &g_broker_conf.lookup_software_script);
	_get_uint32_default(tbl, "LookupTimeoutSec",
			    &g_broker_conf.lookup_timeout_sec,
			    DEFAULT_LOOKUP_TIMEOUT_SEC);
	_get_uint32_default(tbl, "RemoteWorkDirRetentionHours",
			    &g_broker_conf.remote_work_dir_retention_hours,
			    DEFAULT_REMOTE_WORK_DIR_RETENTION_HOURS);
	_get_uint32_default(tbl, "RemoteWorkDirFailureRetentionDays",
			    &g_broker_conf.remote_work_dir_failure_retention_days,
			    DEFAULT_REMOTE_WORK_DIR_FAILURE_RETENTION_DAYS);

	s_p_hashtbl_destroy(tbl);
	_validate_or_die();

	return SLURM_SUCCESS;
}

void broker_conf_log_summary(void)
{
	info("broker_conf: cluster=%s peer=%s:%u default_partition=%s lookup=%s mappings=%u",
	     g_broker_conf.cluster_name,
	     g_broker_conf.remote_broker_host,
	     g_broker_conf.remote_broker_port,
	     g_broker_conf.default_remote_partition,
	     g_broker_conf.lookup_software_script,
	     g_user_mappings ? xhash_count(g_user_mappings) : 0);
}

void broker_conf_fini(void)
{
	xfree(g_broker_conf.cluster_name);
	xfree(g_broker_conf.broker_node_name);
	xfree(g_broker_conf.remote_cluster_name);
	xfree(g_broker_conf.remote_broker_host);
	xfree(g_broker_conf.remote_munge_key_path);
	xfree(g_broker_conf.default_remote_partition);
	xfree(g_broker_conf.auth_type);
	xfree(g_broker_conf.state_save_location);
	xfree(g_broker_conf.state_file_name);
	xfree(g_broker_conf.stage_rsync_bin);
	xfree(g_broker_conf.stage_ssh_key);
	xfree(g_broker_conf.stage_ssh_user);
	xfree(g_broker_conf.lookup_software_script);

	memset(&g_broker_conf, 0, sizeof(g_broker_conf));
}
