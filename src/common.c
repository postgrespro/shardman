/* ------------------------------------------------------------------------
 *
 * common.c
 * 		Common code for ParGRES extension
 *
 * Copyright (c) 2018, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/genam.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "utils/fmgroids.h"

#include "common.h"


MemoryContext DPGMemoryContext = NULL;
ExchangeSharedState *DPGShmem = NULL;
bool enable_distributed_execution;

static bool
plan_walk_members(List *plans, bool (*walker) (), void *context)
{
	ListCell *lc;

	foreach (lc, plans)
	{
		Plan *plan = lfirst(lc);
		if (walker(plan, context))
			return true;
	}

	return false;
}

bool
plan_tree_walker(Plan *plan, bool (*walker) (), void *context)
{
	ListCell   *lc;

	/* Guard against stack overflow due to overly complex plan trees */
	check_stack_depth();

	/* initPlan-s */
	if (plan_walk_members(plan->initPlan, walker, context))
		return true;

	/* lefttree */
	if (outerPlan(plan))
	{
		if (walker(outerPlan(plan), context))
			return true;
	}

	/* righttree */
	if (innerPlan(plan))
	{
		if (walker(innerPlan(plan), context))
			return true;
	}

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			if (plan_walk_members(((ModifyTable *) plan)->plans,
									   walker, context))
				return true;
			break;
		case T_Append:
			if (plan_walk_members(((Append *) plan)->appendplans,
									   walker, context))
				return true;
			break;
		case T_MergeAppend:
			if (plan_walk_members(((MergeAppend *) plan)->mergeplans,
									   walker, context))
				return true;
			break;
		case T_BitmapAnd:
			if (plan_walk_members(((BitmapAnd *) plan)->bitmapplans,
									   walker, context))
				return true;
			break;
		case T_BitmapOr:
			if (plan_walk_members(((BitmapOr *) plan)->bitmapplans,
									   walker, context))
				return true;
			break;
		case T_SubqueryScan:
			if (walker(((SubqueryScan *) plan)->subplan, context))
				return true;
			break;
		case T_CustomScan:
			foreach(lc, ((CustomScan *) plan)->custom_plans)
			{
				if (walker((Plan *) lfirst(lc), context))
					return true;
			}
			break;
		default:
			break;
	}

	return false;
}

static bool
path_walk_members(List *paths, bool (*walker) (), void *context)
{
	ListCell *lc;

	foreach (lc, paths)
	{
		Path *path = lfirst(lc);
		if (walker(path, context))
			return true;
	}

	return false;
}

bool
path_walker(const Path *path, bool (*walker) (), void *context)
{
	Path	*subpath = NULL;
	List	*subpaths = NIL;

	switch (nodeTag(path))
	{
		/*
		 * Extract single sub path.
		 */
		case T_SubqueryScanPath:
			subpath = ((SubqueryScanPath *) path)->subpath;
			break;
		case T_MaterialPath:
			subpath = ((MaterialPath *) path)->subpath;
			break;
		case T_UniquePath:
			subpath = ((UniquePath *) path)->subpath;
			break;
		case T_GatherPath:
			subpath = ((GatherPath *) path)->subpath;
			break;
		case T_GatherMergePath:
			subpath = ((GatherMergePath *) path)->subpath;
			break;
		case T_ProjectionPath:
			subpath = ((ProjectionPath *) path)->subpath;
			break;
		case T_ProjectSetPath:
			subpath = ((ProjectSetPath *) path)->subpath;
			break;
		case T_SortPath:
			subpath = ((SortPath *) path)->subpath;
			break;
		case T_GroupPath:
			subpath = ((GroupPath *) path)->subpath;
			break;
		case T_UpperUniquePath:
			subpath = ((UpperUniquePath *) path)->subpath;
			break;
		case T_AggPath:
			subpath = ((AggPath *) path)->subpath;
			break;
		case T_GroupingSetsPath:
			subpath = ((GroupingSetsPath *) path)->subpath;
			break;
		case T_WindowAggPath:
			subpath = ((WindowAggPath *) path)->subpath;
			break;
		case T_SetOpPath:
			subpath = ((SetOpPath *) path)->subpath;
			break;
		case T_LockRowsPath:
			subpath = ((LockRowsPath *) path)->subpath;
			break;
		case T_LimitPath:
			subpath = ((LimitPath *) path)->subpath;
			break;

		/*
		 * Extract list of sub paths.
		 */
		case T_ModifyTablePath:
			subpaths = ((ModifyTablePath *) path)->subpaths;
			break;
		case T_CustomPath:
			subpaths = ((CustomPath *) path)->custom_paths;
			break;
		case T_AppendPath:
		case T_MergeAppendPath:
			subpaths = ((AppendPath *) path)->subpaths;
			break;

		/*
		 * Extract inner and outer paths of a join
		 */
		case T_NestPath:
		case T_MergePath:
		case T_HashPath:
			if (walker(((JoinPath *) path)->outerjoinpath, context))
				return true;
			if (walker(((JoinPath *) path)->innerjoinpath, context))
				return true;
			break;

		default:
			break;
	}

	if (subpaths != NIL)
		 return path_walk_members(subpaths, walker, context);

	if (subpath != NULL)
		return walker(subpath, context);

	return false;
}

void
OnNodeDisconnect(const char *node_name)
{
	HASH_SEQ_STATUS status;
	DMQDestinations *dest;
	Oid serverid = InvalidOid;

	elog(LOG, "Node %s: disconnected", node_name);


	LWLockAcquire(DPGShmem->lock, LW_EXCLUSIVE);

	hash_seq_init(&status, DPGShmem->htab);

	while ((dest = hash_seq_search(&status)) != NULL)
	{
		if (!(strcmp(dest->node, node_name) == 0))
			continue;

		serverid = dest->serverid;
		dmq_detach_receiver(node_name);
		dmq_destination_drop(node_name);
		break;
	}
	hash_seq_term(&status);

	if (OidIsValid(serverid))
		hash_search(DPGShmem->htab, &serverid, HASH_REMOVE, NULL);
	else
		elog(LOG, "Record on disconnected server %u with name %s not found.",
														serverid, node_name);
	LWLockRelease(DPGShmem->lock);
}
