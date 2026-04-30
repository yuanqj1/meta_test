/*****************************************************************************\
 *  user_mapping.c - slurmbrokerd user mapping table.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP).
\*****************************************************************************/

#include "config.h"

#include <stdbool.h>
#include <string.h>

#include "slurm/slurm.h"

#include "src/common/log.h"
#include "src/common/parse_config.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "user_mapping.h"

xhash_t *g_user_mappings;

static const s_p_options_t mapping_line_options[] = {
	{ "UserMapping", S_P_STRING },
	{ "LocalUser", S_P_STRING },
	{ "RemoteCluster", S_P_STRING },
	{ "RemoteUser", S_P_STRING },
	{ "RemoteUid", S_P_UINT32 },
	{ "RemoteGid", S_P_UINT32 },
	{ NULL }
};

/*
 * Parse broker.conf plus any included user_mapping.conf. Broker keys are
 * accepted and ignored so user_mapping_load() can be called with the same
 * top-level path as broker_conf_init().
 */
static const s_p_options_t mapping_options[] = {
	{ "ClusterName", S_P_IGNORE },
	{ "BrokerNodeName", S_P_IGNORE },
	{ "BrokerCtldPort", S_P_IGNORE },
	{ "BrokerPeerPort", S_P_IGNORE },
	{ "RemoteClusterName", S_P_IGNORE },
	{ "RemoteBrokerHost", S_P_IGNORE },
	{ "RemoteBrokerPort", S_P_IGNORE },
	{ "RemoteMungeKeyPath", S_P_IGNORE },
	{ "DefaultRemotePartition", S_P_IGNORE },
	{ "AuthType", S_P_IGNORE },
	{ "StateSaveLocation", S_P_IGNORE },
	{ "StateFileName", S_P_IGNORE },
	{ "CheckpointInterval", S_P_IGNORE },
	{ "MaxInFlightJobs", S_P_IGNORE },
	{ "MaxStageBytes", S_P_IGNORE },
	{ "PollInterval", S_P_IGNORE },
	{ "PollMaxRetries", S_P_IGNORE },
	{ "StageRsyncBin", S_P_IGNORE },
	{ "StageSshKey", S_P_IGNORE },
	{ "StageSshUser", S_P_IGNORE },
	{ "StageWorkerCount", S_P_IGNORE },
	{ "StageTimeoutPerGB", S_P_IGNORE },
	{ "LookupSoftwareScript", S_P_IGNORE },
	{ "LookupTimeoutSec", S_P_IGNORE },
	{ "RemoteWorkDirRetentionHours", S_P_IGNORE },
	{ "RemoteWorkDirFailureRetentionDays", S_P_IGNORE },
	{ "UserMapping", S_P_LINE, NULL, NULL, mapping_line_options },
	{ NULL }
};

static void _mapping_idfunc(void *item, const char **key, uint32_t *len)
{
	user_mapping_t *mapping = item;

	*key = mapping->hash_key;
	*len = strlen(mapping->hash_key);
}

static void _mapping_freefunc(void *item)
{
	user_mapping_t *mapping = item;

	if (!mapping)
		return;

	xfree(mapping->local_user);
	xfree(mapping->remote_cluster);
	xfree(mapping->remote_user);
	xfree(mapping->hash_key);
	xfree(mapping);
}

static char *_build_key(const char *local_user, const char *remote_cluster)
{
	char *key = NULL;

	xstrfmtcat(key, "%s|%s", local_user, remote_cluster);
	return key;
}

static void _get_mapping_string_required(s_p_hashtbl_t *tbl, const char *key,
					 char **field, int idx)
{
	if (!s_p_get_string(field, key, tbl) || !*field || !**field)
		fatal("user_mapping.conf: UserMapping[%d] missing required '%s'",
		      idx, key);
}

static void _load_one_mapping(s_p_hashtbl_t *line, int idx)
{
	user_mapping_t *mapping;
	bool have_uid, have_gid;

	mapping = xmalloc(sizeof(*mapping));
	if (!s_p_get_string(&mapping->local_user, "LocalUser", line) ||
	    !mapping->local_user || !*mapping->local_user)
		_get_mapping_string_required(line, "UserMapping",
					     &mapping->local_user, idx);
	_get_mapping_string_required(line, "RemoteCluster",
				     &mapping->remote_cluster, idx);
	_get_mapping_string_required(line, "RemoteUser", &mapping->remote_user,
				     idx);
	have_uid = s_p_get_uint32(&mapping->remote_uid, "RemoteUid", line);
	have_gid = s_p_get_uint32(&mapping->remote_gid, "RemoteGid", line);
	if (!have_uid || !have_gid) {
		_mapping_freefunc(mapping);
		fatal("user_mapping.conf: UserMapping[%d] missing RemoteUid/RemoteGid",
		      idx);
	}

	mapping->hash_key = _build_key(mapping->local_user,
				       mapping->remote_cluster);
	if (xhash_get_str(g_user_mappings, mapping->hash_key)) {
		char *dup_key = xstrdup(mapping->hash_key);

		_mapping_freefunc(mapping);
		fatal("user_mapping.conf: duplicate mapping key '%s'", dup_key);
	}

	xhash_add(g_user_mappings, mapping);
}

int user_mapping_load(const char *path)
{
	s_p_hashtbl_t *tbl;
	s_p_hashtbl_t **lines = NULL;
	int line_count = 0;

	if (!path)
		fatal("user_mapping.conf: no config path supplied");

	user_mapping_destroy_all();
	g_user_mappings = xhash_init(_mapping_idfunc, _mapping_freefunc);
	if (!g_user_mappings)
		fatal("user_mapping.conf: xhash_init failed");

	tbl = s_p_hashtbl_create(mapping_options);
	if (s_p_parse_file(tbl, NULL, (char *)path, 0, NULL) == SLURM_ERROR) {
		s_p_hashtbl_destroy(tbl);
		fatal("user_mapping.conf: parse failed for %s", path);
	}

	if (s_p_get_line(&lines, &line_count, "UserMapping", tbl)) {
		for (int i = 0; i < line_count; i++)
			_load_one_mapping(lines[i], i);
	}

	s_p_hashtbl_destroy(tbl);
	return SLURM_SUCCESS;
}

user_mapping_t *user_mapping_lookup(const char *local_user,
				    const char *remote_cluster)
{
	user_mapping_t *mapping;
	char *key;

	if (!g_user_mappings || !local_user || !remote_cluster)
		return NULL;

	key = _build_key(local_user, remote_cluster);
	mapping = xhash_get_str(g_user_mappings, key);
	xfree(key);

	return mapping;
}

void user_mapping_destroy_all(void)
{
	xhash_free(g_user_mappings);
}
