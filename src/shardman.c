/* -------------------------------------------------------------------------
 *
 * shardman.c
 *
 * Copyright (c) 2018, Postgres Professional
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/global_snapshot.h"
#include "catalog/namespace.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "utils/fmgroids.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postgres_fdw.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/guc.h"

#include "common.h"
#include "dmq.h"
#include "hooks.h"
#include "meta.h"
#include "planpass.h"
#include "shardman.h"

/* ensure that extension won't load against incompatible version of Postgres */
PG_MODULE_MAGIC;

/* SQL funcs */
PG_FUNCTION_INFO_V1(ex_sql);
PG_FUNCTION_INFO_V1(bcst_sql);
PG_FUNCTION_INFO_V1(bcst_all_sql);
PG_FUNCTION_INFO_V1(generate_global_snapshot);
PG_FUNCTION_INFO_V1(get_instr_time);

/* Execute portable query plan */
PG_FUNCTION_INFO_V1(pg_exec_plan);


/* GUC variables */
int MyRgid;
static bool broadcast_utility;
static bool in_broadcast;

static bool AmCoordinator(void);
static void ShmnProcessUtility(PlannedStmt *pstmt,
							   const char *queryString, ProcessUtilityContext context,
							   ParamListInfo params,
							   QueryEnvironment *queryEnv,
							   DestReceiver *dest, char *completionTag);
static bool BroadcastNeeded(PlannedStmt *pstmt,
							const char *queryString, ProcessUtilityContext context,
							ParamListInfo params,
							QueryEnvironment *queryEnv,
							DestReceiver *dest, char *completionTag);
static void Ex(int rgid, char *sql);
static void ExLocal(char *sql);
static void BcstAll(char *sql);
static void Bcst(char *sql);
static void ExServer(Oid serverid, char *sql);
static void do_sql_command(PGconn *conn, char *sql);
static bool ShardmanLoaded(void);

static ProcessUtility_hook_type PreviousProcessUtilityHook;


/*
 * Entrypoint of the module. Define GUCs.
 */
void
_PG_init()
{
	DefineCustomIntVariable(
		"shardman.rgid",
		"My replication group id",
		NULL,
		&MyRgid,
		-1, -1, 4096, /* boot, min, max */
		PGC_SUSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"shardman.broadcast_utility",
		"When off, no utility statements are broadcasted",
		"shardman will broadcast to all repgroups utility statements which it supports and for which it does make sense. You can disable/enable this at any time.",
		&broadcast_utility,
		true,
		PGC_USERSET,
		0, /* flags */
		NULL, NULL, NULL); /* hooks */

	DefineCustomBoolVariable("enable_distributed_execution",
							 "Use distributed execution.",
							 NULL,
							 &enable_distributed_execution,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("dmq_heartbeat_timeout",
							"Max timeout between heartbeat messages",
							NULL,
							&dmq_heartbeat_timeout,
							20000,
							1, INT_MAX,
							PGC_USERSET,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	/* Install hooks */
	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = ShmnProcessUtility;
	EXEC_Hooks_init();
	dmq_init("shardman");
}

static bool AmCoordinator(void)
{
	return strstr(application_name, "pgfdw:") == NULL &&
		/*
		 * Temporary plaster for vanilla postgres_fdw; we anyway need more advanced
		 * application_name for deadlock detector.
		 */
		strstr(application_name, "postgres_fdw") == NULL;
}

static void ShmnProcessUtility(PlannedStmt *pstmt,
							 const char *queryString, ProcessUtilityContext context,
							 ParamListInfo params,
							 QueryEnvironment *queryEnv,
							 DestReceiver *dest, char *completionTag)
{
	bool broadcast = false;
	bool consider_broadcast;
	Node *parsetree = pstmt->utilityStmt;
	int stmt_start = pstmt->stmt_location > 0 ? pstmt->stmt_location : 0;
	int stmt_len = pstmt->stmt_len > 0 ? pstmt->stmt_len : strlen(queryString + stmt_start);
	char *stmt_string = palloc(stmt_len + 1);

	/*
	 * in_broadcast protects against repeated broadcast for the same statement
	 * with multiple ProcessUtility's, e.g. create table with primary key.
	 */
	if (!ShardmanLoaded() || in_broadcast)
	{
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
		return;
	}
	in_broadcast = true;

	strncpy(stmt_string, queryString + stmt_start, stmt_len);
	stmt_string[stmt_len] = '\0';
	shmn_log3("SHMNProcessUtility stmt %s, node %s, coordinator %d", stmt_string, nodeToString(parsetree), AmCoordinator());

	/*
	 * We broadcast only in these cases. No need to broadcast extenstion
	 * objects, we broadcast CREATE EXTENSTION itself.
	 */
	consider_broadcast = AmCoordinator() && broadcast_utility &&
		!creating_extension && MyRgid != -1;

	if (consider_broadcast)
		broadcast = BroadcastNeeded(pstmt, queryString, context, params,
									queryEnv, dest, completionTag);

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
		shmn_log1("Broadcasting stmt %s, node %s", stmt_string, nodeToString(parsetree));
		Bcst(stmt_string);
	}
	in_broadcast = false;
}

static bool BroadcastNeeded(PlannedStmt *pstmt,
							 const char *queryString, ProcessUtilityContext context,
							 ParamListInfo params,
							 QueryEnvironment *queryEnv,
							 DestReceiver *dest, char *completionTag)
{
	Node *parsetree = pstmt->utilityStmt;

	// TODO TODO TODO
	switch (nodeTag(parsetree))
	{
		case T_AlterTableStmt:
		case T_AlterDomainStmt:
		case T_GrantStmt:
		case T_GrantRoleStmt:
		case T_ClusterStmt:
			{
				return true;
			}

		case T_CreateStmt:
			{
				/* skip creation of sharded table's partition */
				CreateStmt *stmt = (CreateStmt *) parsetree;

				if (stmt->partbound)
				{
					RangeVar *parent = linitial_node(RangeVar, stmt->inhRelations);
					Oid parent_oid = RangeVarGetRelid(parent, NoLock, false);

					if (RelIsSharded(parent_oid))
						return false;
				}

				return true;
			}

		case T_DefineStmt:
		{
			return true;
		}

		case T_DropStmt:
			{
				DropStmt *stmt = (DropStmt *) parsetree;

				if (stmt->removeType == OBJECT_EXTENSION)
				{
					ListCell   *cell1;

					foreach(cell1, stmt->objects)
					{
						Node	   *object = lfirst(cell1);

						if (strcmp(strVal((Value *) object), "shardman") == 0)
							return false;
					}
				} else if (stmt->removeType == OBJECT_FOREIGN_SERVER)
				{
					return false; /* xxx allow user foreign servers */
				} else if (stmt->removeType == OBJECT_TABLE)
				{
					ListCell *cell;

					/* When removing sharded table, purge metadata */
					foreach(cell, stmt->objects)
					{
						RangeVar *rv = makeRangeVarFromNameList((List *) lfirst(cell));
						Oid relid = RangeVarGetRelid(rv, AccessExclusiveLock, true);
						if (relid != InvalidOid && RelIsSharded(relid))
						{
							DropShardedRel(relid);
						}
					}
				}

				return true;
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
		/* case T_CreatedbStmt: */
		/* case T_DropdbStmt: */
		/* case T_VacuumStmt: */
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
		{
			/* Those are mostly fine to broadcast */
			return true;
		}

		default:
			return false;
	}
}


Datum
ex_sql(PG_FUNCTION_ARGS) {
	int rgid = PG_GETARG_INT32(0);
	char* sql = text_to_cstring(PG_GETARG_TEXT_PP(1));
	Ex(rgid, sql);
	PG_RETURN_VOID();
}

Datum
bcst_sql(PG_FUNCTION_ARGS) {
	char* sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	Bcst(sql);
	PG_RETURN_VOID();
}

Datum bcst_all_sql(PG_FUNCTION_ARGS) {
	char *sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	BcstAll(sql);
	PG_RETURN_VOID();
}

/* execute no-data-returning cmd on given rgid */
static void Ex(int rgid, char *sql)
{
	bool isnull;
	Oid serverid;

	serverid = ServerIdByRgid(rgid, &isnull);
	if (isnull)
	{
		/* execute on myself */
		Assert(rgid == MyRgid);
		ExLocal(sql);
		return;
	}

	/* external */
	shmn_log2("executing cmd %s on external rgid %d", sql, rgid);
	ExServer(serverid, sql);
}

/* execute command locally via SPI */
static void ExLocal(char *sql)
{
	int ret;

	shmn_log2("executing local cmd %s", sql);
	if ((ret = SPI_connect()) != SPI_OK_CONNECT)
	{
		shmn_elog(ERROR, "SPI_connect failed: %d", ret);
	}

	if ((ret = SPI_exec(sql, 0)) < 0)
	{
		shmn_elog(ERROR, "SPI_exec failed: %d", ret);
	}
	SPI_finish();
}

/* execute command on all repgroups, including myself */
static void BcstAll(char *sql)
{
	/* execute on me */
	ExLocal(sql);
	/* and everywhere else */
	Bcst(sql);
}

/* execute cmd on all *external* repgroups */
static void Bcst(char *sql)
{
	Relation rel;
	SysScanDesc scan;
	TupleDesc tupleDescriptor;
	HeapTuple tuple;

	rel = heap_open(RepgroupsOid(), AccessShareLock);
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
		if (!isnull)
			ExServer(serverid, sql);
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);
}

/* execute cmd on given foreign server */
static void ExServer(Oid serverid, char *sql)
{
	UserMapping *user;
	PGconn *conn;
	char *spathcmd;

	user = GetUserMapping(GetUserId(), serverid);
	conn = GetConnection(user, false);
	pfree(user);

	/*
	 * postgres_fdw relies on search path being "pg_catalog", set current one
	 * and restore it back later
	 */
	spathcmd = psprintf("set search_path = %s", namespace_search_path);
	do_sql_command(conn, spathcmd);
	pfree(spathcmd);
	do_sql_command(conn, sql);
	do_sql_command(conn, "set search_path = pg_catalog");
}

/* we could export this from connection.c */
static void do_sql_command(PGconn *conn, char *sql)
{
	PGresult *res;

	if (!PQsendQuery(conn, sql))
		pgfdw_report_error(ERROR, NULL, conn, false, sql);
	res = pgfdw_get_result(conn, sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK &&
		PQresultStatus(res) != PGRES_TUPLES_OK)
		pgfdw_report_error(ERROR, res, conn, true, sql);
	PQclear(res);
}

/* TODO: caching */
static bool ShardmanLoaded()
{
	Oid extension_oid = get_extension_oid("shardman", true);
	return extension_oid != InvalidOid;
}

Datum
pg_exec_plan(PG_FUNCTION_ARGS)
{
	char	*squery = TextDatumGetCString(PG_GETARG_DATUM(0)),
			*splan = TextDatumGetCString(PG_GETARG_DATUM(1)),
			*sparams = TextDatumGetCString(PG_GETARG_DATUM(2)),
			*serverName = TextDatumGetCString(PG_GETARG_DATUM(3)),
			*start_address;
	PlannedStmt *pstmt;
	ParamListInfo paramLI;

	deserialize_plan(&squery, &splan, &sparams);

	pstmt = (PlannedStmt *) stringToNode(splan);

	/* Deserialize parameters of the query */
	start_address = sparams;
	paramLI = RestoreParamList(&start_address);

	exec_plan(squery, pstmt, paramLI, serverName);
	PG_RETURN_VOID();
}

/* For debugging */
Datum
generate_global_snapshot(PG_FUNCTION_ARGS)
{
	GlobalCSN snapshot = GlobalSnapshotGenerate(false);

	PG_RETURN_UINT64(snapshot);
}
Datum
get_instr_time(PG_FUNCTION_ARGS)
{
	instr_time	current_time;
	uint64 tm_nsec;

	INSTR_TIME_SET_CURRENT(current_time);
	tm_nsec = INSTR_TIME_GET_NANOSEC(current_time);
	PG_RETURN_UINT64(tm_nsec);
}
