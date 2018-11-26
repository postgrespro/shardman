/* -------------------------------------------------------------------------
 *
 * meta.c
 *   Handles metadata.
 *
 * Copyright (c) 2018, Postgres Professional
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "meta.h"

#define Natts_sharded_tables				3
#define Anum_sharded_tables_rel				1
#define Anum_sharded_tables_nparts			2
#define Anum_sharded_tables_colocated_with	3

static Oid HpNamespaceOid(void);
static Oid RepgroupsIndexOid(void);
static Oid ShardedTablesOid(void);

/* TODO: caching */
static Oid HpNamespaceOid(void)
{
	return get_namespace_oid("hodgepodge", false);
}

Oid RepgroupsOid(void)
{
	return get_relname_relid("repgroups", HpNamespaceOid());
}

static Oid RepgroupsIndexOid(void)
{
	return get_relname_relid("repgroups_pkey", HpNamespaceOid());
}

static Oid ShardedTablesOid(void)
{
	return get_relname_relid("sharded_tables", HpNamespaceOid());
}

static Oid ShardedTablesIndexOid(void)
{
	return get_relname_relid("sharded_tables_pkey", HpNamespaceOid());
}

/* Get foreign server oid by rgid. Errors out if there is no such rgid. */
Oid ServerIdByRgid(int rgid, bool *isnull)
{
	Relation rel;
	SysScanDesc scan_desc;
	ScanKeyData key[1];
	HeapTuple repgroup_tuple;
	TupleDesc tupleDescriptor;
	Datum serverid_datum;
	Oid serverid;

	rel = heap_open(RepgroupsOid(), AccessShareLock);
	tupleDescriptor = RelationGetDescr(rel);
	/* find tuple */
	ScanKeyInit(&key[0],
				Anum_repgroups_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(rgid));

	scan_desc = systable_beginscan(rel,
								   RepgroupsIndexOid(),
								   true, NULL, 1, key);

	repgroup_tuple = systable_getnext(scan_desc);
	if (HeapTupleIsValid(repgroup_tuple))
	{
		serverid_datum = heap_getattr(repgroup_tuple,
									  Anum_repgroups_srvid,
									  tupleDescriptor,
									  isnull);
		serverid = DatumGetObjectId(serverid_datum);
	}
	else
	{
		elog(ERROR, "repgroup with id %d not found", rgid);
	}

	systable_endscan(scan_desc);
	heap_close(rel, AccessShareLock);
	return serverid;
}

/* check that rel is sharded */
bool RelIsSharded(Oid relid)
{
	Relation rel;
	SysScanDesc scan_desc;
	ScanKeyData key[1];
	bool found = false;
	HeapTuple tuple;

	rel = heap_open(ShardedTablesOid(), AccessShareLock);
	/* find tuple */
	ScanKeyInit(&key[0],
				Anum_sharded_tables_rel,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	scan_desc = systable_beginscan(rel,
								   ShardedTablesIndexOid(),
								   true, NULL, 1, key);

	tuple = systable_getnext(scan_desc);
	if (HeapTupleIsValid(tuple))
	{
		found = true;
	}

	systable_endscan(scan_desc);
	heap_close(rel, AccessShareLock);
	return found;
}
