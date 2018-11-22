/* -------------------------------------------------------------------------
 *
 * hodgepodge.c
 *
 * Copyright (c) 2018, Postgres Professional
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "commands/extension.h"
#include "utils/fmgroids.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/guc.h"

#include "postgres_fdw/postgres_fdw.h"

/* ensure that extension won't load against incompatible version of Postgres */
PG_MODULE_MAGIC;

/* SQL funcs */
PG_FUNCTION_INFO_V1(ex_sql);
PG_FUNCTION_INFO_V1(bcst_sql);

/* GUC variables */
static int node_id;
static bool broadcast_utility;

extern void _PG_init(void);

static bool am_coordinator(void);
static void HPProcessUtility(PlannedStmt *pstmt,
							 const char *queryString, ProcessUtilityContext context,
							 ParamListInfo params,
							 QueryEnvironment *queryEnv,
							 DestReceiver *dest, char *completionTag);
static Oid hp_namespace_oid(void);
static Oid repgroups_oid(void);
static void ex(int rgid, char *sql);
static void bcst(char *sql);
static void ex_server(Oid serverid, char *sql);

static ProcessUtility_hook_type PreviousProcessUtilityHook;

#ifndef DEBUG_LEVEL
#define DEBUG_LEVEL 0
#endif

#define HP_TAG "[HP] "
#define hp_elog(level,fmt,...) elog(level, HP_TAG fmt, ## __VA_ARGS__)

#if DEBUG_LEVEL == 0
#define hp_log1(fmt, ...) elog(LOG, HP_TAG fmt, ## __VA_ARGS__)
#define hp_log2(fmt, ...)
#elif DEBUG_LEVEL == 1
#define hp_log1(fmt, ...) elog(LOG, HP_TAG fmt, ## __VA_ARGS__)
#define hp_log2(fmt, ...) elog(LOG, HP_TAG fmt, ## __VA_ARGS__)
#endif

#define Natts_repgroups				2
#define Anum_repgroups_id			1
#define Anum_repgroups_srvid		2	/* partition expression (original) */


/*
 * Entrypoint of the module. Define GUCs.
 */
void
_PG_init()
{
	DefineCustomIntVariable(
		"hodgepodge.rgid",
		"My replication group id",
		NULL,
		&node_id,
		-1, -1, 4096, /* boot, min, max */
		PGC_SUSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"hodgepodge.broadcast_utility",
		"When off, no utility statements are broadcasted",
		"hodgepodge will broadcast to all repgroups utility statements which it supports and for which it does make sense. You can disable/enable this at any time.",
		&broadcast_utility,
		true,
		PGC_USERSET,
		0, /* flags */
		NULL, NULL, NULL); /* hooks */

	/* Install hooks */
	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = HPProcessUtility;

	postgres_fdw_PG_init();
}

Datum
ex_sql(PG_FUNCTION_ARGS) {
	int rgid = PG_GETARG_INT32(0);
	char* sql = text_to_cstring(PG_GETARG_TEXT_PP(1));
	ex(rgid, sql);
	PG_RETURN_VOID();
}

Datum
bcst_sql(PG_FUNCTION_ARGS) {
	char* sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bcst(sql);
	PG_RETURN_VOID();
}

static bool am_coordinator(void)
{
	return strstr(application_name, "pgfdw:") == NULL;
}

static void HPProcessUtility(PlannedStmt *pstmt,
							 const char *queryString, ProcessUtilityContext context,
							 ParamListInfo params,
							 QueryEnvironment *queryEnv,
							 DestReceiver *dest, char *completionTag)
{
	bool broadcast = false;
	Node *parsetree = pstmt->utilityStmt;
	int stmt_start = pstmt->stmt_location > 0 ? pstmt->stmt_location : 0;
	int stmt_len = pstmt->stmt_len > 0 ? pstmt->stmt_len : strlen(queryString + stmt_start);
	char *stmt_string = palloc(stmt_len + 1);

	strncpy(stmt_string, queryString + stmt_start, stmt_len);
	stmt_string[stmt_len] = '\0';

	// TODO TODO TODO
	switch (nodeTag(parsetree))
	{
		case T_AlterTableStmt:
		case T_AlterDomainStmt:
		case T_GrantStmt:
		case T_GrantRoleStmt:
		case T_ClusterStmt:
		case T_CreateStmt:
		case T_DefineStmt:
			if (am_coordinator() && broadcast_utility && !creating_extension) {
				broadcast = true;
			}
			break;

		case T_DropStmt:
			{
				DropStmt *stmt = (DropStmt *) parsetree;

				if (stmt->removeType == OBJECT_EXTENSION)
				{
					ListCell   *cell1;

					foreach(cell1, stmt->objects) {
						Node	   *object = lfirst(cell1);

						if (strcmp(strVal((Value *) object), "hodgepodge") == 0)
							goto end_of_switch;
					}
				} else if (stmt->removeType == OBJECT_FOREIGN_SERVER)
				{
					goto end_of_switch;
				}

				if (am_coordinator() && broadcast_utility && !creating_extension) {
					broadcast = true;
				}
			}
			break;

		case T_CommentStmt:
		case T_IndexStmt:
		case T_CreateFunctionStmt:
		case T_AlterFunctionStmt:
		case T_RenameStmt:
		case T_RuleStmt:
		case T_ViewStmt:
		case T_LoadStmt:
		case T_CreateDomainStmt:
		case T_CreatedbStmt:
		case T_DropdbStmt:
		case T_VacuumStmt:
		case T_CreateTableAsStmt:
		case T_CreateTrigStmt:
		case T_CreatePLangStmt:
		case T_CreateRoleStmt:
		case T_AlterRoleStmt:
		case T_DropRoleStmt:
		/* case T_LockStmt: */
		case T_ConstraintsSetStmt:
		case T_ReindexStmt:
		case T_CheckPointStmt:
		case T_CreateSchemaStmt:
		case T_AlterDatabaseStmt:
		case T_AlterDatabaseSetStmt:
		case T_AlterRoleSetStmt:
		case T_CreateConversionStmt:
		case T_CreateCastStmt:
		case T_CreateOpClassStmt:
		case T_CreateOpFamilyStmt:
		case T_AlterOpFamilyStmt:
		case T_CreateTableSpaceStmt:
		case T_DropTableSpaceStmt:
		case T_AlterObjectDependsStmt:
		case T_AlterObjectSchemaStmt:
		case T_AlterOwnerStmt:
		case T_AlterOperatorStmt:
		case T_DropOwnedStmt:
		case T_ReassignOwnedStmt:
		case T_CompositeTypeStmt:
		case T_CreateEnumStmt:
		case T_CreateRangeStmt:
		case T_AlterEnumStmt:
		case T_AlterTSDictionaryStmt:
		case T_AlterTSConfigurationStmt:
		case T_AlterTableSpaceOptionsStmt:
		case T_AlterTableMoveAllStmt:
		case T_SecLabelStmt:
		case T_CreateExtensionStmt:
		case T_AlterExtensionStmt:
		case T_AlterExtensionContentsStmt:
		case T_CreateEventTrigStmt:
		case T_AlterEventTrigStmt:
		case T_RefreshMatViewStmt:
		case T_ReplicaIdentityStmt:
		case T_AlterSystemStmt:
		case T_CreatePolicyStmt:
		case T_AlterPolicyStmt:
		case T_CreateTransformStmt:
		case T_CreateAmStmt:
		case T_CreateStatsStmt:
		case T_AlterCollationStmt:
			if (am_coordinator() && broadcast_utility && !creating_extension) {
				broadcast = true;
			}
			break;

		default:
			break;
	}
end_of_switch:

	if (PreviousProcessUtilityHook != NULL)
	{
		PreviousProcessUtilityHook(pstmt, queryString,
										 context, params, queryEnv,
										 dest, completionTag);
	}
	else
	{
		standard_ProcessUtility(pstmt, queryString,
									context, params, queryEnv,
									dest, completionTag);
	}

	if (broadcast)
	{
		hp_log1("Broadcasting stmt %s, node %s", stmt_string, nodeToString(parsetree));
		bcst(stmt_string);

	}
}


/* TODO: caching */
static Oid hp_namespace_oid(void)
{
	return get_namespace_oid("hodgepodge", false);
}

static Oid repgroups_oid(void)
{
	return get_relname_relid("repgroups", hp_namespace_oid());
}

static Oid repgroups_index_oid(void)
{
	return get_relname_relid("repgroups_pkey", hp_namespace_oid());
}

/* execute no-data-returning cmd on given external rgid */
static void ex(int rgid, char *sql)
{
	Relation rel;
	SysScanDesc scan_desc;
	ScanKeyData key[1];
	HeapTuple repgroup_tuple;
	TupleDesc tupleDescriptor;
	bool isnull;
	Datum serverid_datum;
	Oid serverid;

	rel = heap_open(repgroups_oid(), AccessShareLock);
	/* find tuple */
	ScanKeyInit(&key[0],
				Anum_repgroups_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(rgid));

	scan_desc = systable_beginscan(rel,
										repgroups_index_oid(),
										true, NULL, 1, key);

	repgroup_tuple = systable_getnext(scan_desc);
	if (HeapTupleIsValid(repgroup_tuple))
	{
		/* let's practice a bit in copying tuples */
		repgroup_tuple = heap_copytuple(repgroup_tuple);
	}
	else
	{
		elog(ERROR, "external repgroup with id %d not found", rgid);
	}

	systable_endscan(scan_desc);
	heap_close(rel, AccessShareLock);

	/* make sense of it */
	tupleDescriptor = RelationGetDescr(rel);
	serverid_datum = heap_getattr(repgroup_tuple,
							   Anum_repgroups_srvid,
							   tupleDescriptor,
							   &isnull);
	if (isnull)
	{
		elog(ERROR, "srv for rgid %d not defined", rgid);
	}
	serverid = DatumGetObjectId(serverid_datum);
	heap_freetuple(repgroup_tuple);

	ex_server(serverid, sql);
}

/* execute cmd on all external repgroups */
static void bcst(char *sql)
{
	Relation rel;
	SysScanDesc scan;
	TupleDesc tupleDescriptor;
	HeapTuple tuple;

	rel = heap_open(repgroups_oid(), AccessShareLock);
	tupleDescriptor = RelationGetDescr(rel);

	scan = systable_beginscan(rel, 0, true, NULL, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Oid serverid;
		bool isnull;

		serverid = DatumGetObjectId(heap_getattr(tuple,
												 Anum_repgroups_srvid,
												 tupleDescriptor,
												 &isnull));
		/* no good to run cmd here: if we fail, rel leaks, so first read everything */
		ex_server(serverid, sql);
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);
}

/* execute cmd on given foreign server */
static void ex_server(Oid serverid, char *sql)
{
	UserMapping *user;
	ConnCacheEntry *entry;
	PGconn *conn;
	PGresult *res;

	user = GetUserMapping(GetUserId(), serverid);
	entry = GetConnection(user, false);
	pfree(user);
	conn = ConnectionEntryGetConn(entry);

	if (!PQsendQuery(conn, sql))
		pgfdw_report_error(ERROR, NULL, conn, false, sql);
	res = pgfdw_get_result(entry, sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK &&
		PQresultStatus(res) != PGRES_TUPLES_OK)
		pgfdw_report_error(ERROR, res, conn, true, sql);
	PQclear(res);
}
