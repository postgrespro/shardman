/* -------------------------------------------------------------------------
 *
 * meta.h
 *   Handles metadata.
 *
 * Copyright (c) 2018, Postgres Professional
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

/* xxx hide this */
#define Natts_repgroups				2
#define Anum_repgroups_id			1
#define Anum_repgroups_srvid		2


extern Oid ServerIdByRgid(int rgid, bool *isnull);

extern Oid RepgroupsOid(void);
extern bool RelIsSharded(Oid rel);
extern void DropShardedRel(Oid rel);
