/*****************************************************************************\
 *  user_mapping.h - slurmbrokerd user mapping table.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP).
\*****************************************************************************/

#ifndef _USER_MAPPING_H
#define _USER_MAPPING_H

#include <stdint.h>

#include "src/common/xhash.h"

typedef struct {
	char *local_user;
	char *remote_cluster;
	char *remote_user;
	uint32_t remote_uid;
	uint32_t remote_gid;

	/* xhash stores a pointer to this key; it must live with the item. */
	char *hash_key;
} user_mapping_t;

extern xhash_t *g_user_mappings;

extern int user_mapping_load(const char *path);
extern user_mapping_t *user_mapping_lookup(const char *local_user,
					   const char *remote_cluster);
extern void user_mapping_destroy_all(void);

#endif /* _USER_MAPPING_H */
