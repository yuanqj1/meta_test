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
		fatal("broker.conf: UserMapping[%d] missing required '%s'",
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
		fatal("broker.conf: UserMapping[%d] missing RemoteUid/RemoteGid",
		      idx);
	}

	mapping->hash_key = _build_key(mapping->local_user,
				       mapping->remote_cluster);
	if (xhash_get_str(g_user_mappings, mapping->hash_key)) {
		char *dup_key = xstrdup(mapping->hash_key);

		_mapping_freefunc(mapping);
		fatal("broker.conf: duplicate mapping key '%s'", dup_key);
	}

	xhash_add(g_user_mappings, mapping);
}

int user_mapping_load_from_hashtbl(s_p_hashtbl_t *tbl)
{
	s_p_hashtbl_t **lines = NULL;
	int line_count = 0;

	if (!tbl)
		fatal("broker.conf: internal error (null user mapping table)");

	user_mapping_destroy_all();
	g_user_mappings = xhash_init(_mapping_idfunc, _mapping_freefunc);
	if (!g_user_mappings)
		fatal("broker.conf: xhash_init failed for user mappings");

	if (s_p_get_line(&lines, &line_count, "UserMapping", tbl)) {
		for (int i = 0; i < line_count; i++)
			_load_one_mapping(lines[i], i);
	}

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
