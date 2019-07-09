/* ------------------------------------------------------------------------
 *
 * hooks_exec.c
 *		Executor-related logic of the ParGRES extension.
 *
 * Copyright (c) 2018, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h" /* for check_stack_depth() */
#include "optimizer/pathnode.h"
#include "optimizer/paths.h" /* Need for paths hooks */
#include "optimizer/planner.h"
#include "storage/ipc.h"
#include "unistd.h"

#include "common.h"
#include "dmq.h"
#include "distpaths.h"
#include "expath.h"
#include "hooks.h"
#include "nodeDistPlanExec.h"
#include "partutils.h"
#include "stream.h"


static set_rel_pathlist_hook_type	prev_set_rel_pathlist_hook = NULL;
static shmem_startup_hook_type		PreviousShmemStartupHook = NULL;
static set_join_pathlist_hook_type	prev_set_join_pathlist_hook = NULL;
static dmq_receiver_hook_type		old_dmq_receiver_stop_hook = NULL;
static create_upper_paths_hook_type	prev_create_upper_paths_hook = NULL;

static void HOOK_Baserel_paths(PlannerInfo *root, RelOptInfo *rel, Index rti,
															RangeTblEntry *rte);
static void HOOK_Upper_paths(PlannerInfo *root, UpperRelationKind stage,
					RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra);
static void HOOK_Join_pathlist(PlannerInfo *root, RelOptInfo *joinrel,
							   RelOptInfo *outerrel, RelOptInfo *innerrel,
							   JoinType jointype, JoinPathExtraData *extra);


/*
 * Make distributed paths for scan operation of partitioned relation.
 * Restrictions: at least one partition must be placed in the coordinator's
 * storage.
 */
static void
HOOK_Baserel_paths(PlannerInfo *root, RelOptInfo *rel, Index rti,
															RangeTblEntry *rte)
{
	List		*pathlist = NIL;
	ListCell	*lc;

	if (prev_set_rel_pathlist_hook)
		(*prev_set_rel_pathlist_hook) (root, rel, rti, rte);

	/* Not create distributed paths if the GUC is disabled */
	if (!enable_distributed_execution)
		return;

	/* Get the list of distributed paths.*/
	pathlist = distributedscan_pathlist(root, rel, rte);

	/* Insert each distributed path into the relation pathlist */
	foreach(lc, pathlist)
		add_path(rel, (Path *) lfirst(lc));

	list_free(pathlist);
}

/*
 * We need it to reflect apply_scanjoin_target_to_paths() code.
 */
static void
HOOK_Upper_paths(PlannerInfo *root, UpperRelationKind stage,
				 RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra)
{
	bool rel_is_partitioned = IS_PARTITIONED_REL(input_rel);

	if (prev_create_upper_paths_hook)
		(*prev_create_upper_paths_hook)(root, stage, input_rel, output_rel, extra);

	if (!enable_distributed_execution)
		return;

	return;

	if (rel_is_partitioned)
		HOOK_Baserel_paths(root, input_rel, input_rel->relid,
									root->simple_rte_array[input_rel->relid]);
//	debug_print_rel(root, input_rel);
//	debug_print_rel(root, output_rel);
	output_rel->pathlist = input_rel->pathlist;
}

static bool
IsDispatcherNode(const Path *path, const void *context)
{
	if (IsDistExecNode(path))
		return true;
	return false;
}

/*
 * If left and right relations are partitioned (XXX: not only) we can use an
 * exchange node as left or right son for tuples shuffling of a relation in
 * accordance with partitioning scheme of another relation.
 */
static void
HOOK_Join_pathlist(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
		 	 	   RelOptInfo *innerrel, JoinType jointype,
				   JoinPathExtraData *extra)
{
	List		*paths;
	List		*bad_paths = NIL;
	ListCell	*lc;

	if (prev_set_join_pathlist_hook)
			(*prev_set_join_pathlist_hook)(root, joinrel, outerrel, innerrel,
															jointype, extra);
//elog(INFO, "list len: %d", list_length(joinrel->pathlist));
	/*
	 * Delete all bad paths.
	 * It is paths with DISPATCH node at inner and outer subtrees.
	 * We can't execute the plan because FDW has only one connection to one
	 * foreign server and it is impossible to launch another query before end
	 * of previous query.
	 */
	foreach(lc, joinrel->pathlist)
	{
		Path		*path = lfirst(lc);
		JoinPath	*jp = NULL;

		if (IsDistExecNode(path))
			continue;

		if (nodeTag(path) == T_NestPath || nodeTag(path) == T_MergePath ||
			nodeTag(path) == T_HashPath)
			jp = (JoinPath *) path;

		if (jp && path_walker(jp->innerjoinpath, IsDispatcherNode, NULL)
			&& path_walker(jp->outerjoinpath, IsDispatcherNode, NULL))
			bad_paths = lappend(bad_paths, path);
	}

	foreach(lc, bad_paths)
		joinrel->pathlist = list_delete_ptr(joinrel->pathlist, lfirst(lc));
	list_free(bad_paths);

	paths = create_distributed_join_paths(root, joinrel, outerrel, innerrel,
															jointype, extra);
	foreach(lc, paths)
		add_path(joinrel, (Path *) lfirst(lc));
	list_free(paths);
}

static Size
shmem_size(void)
{
	Size	size = 0;

	size = add_size(size, sizeof(ExchangeSharedState));
	size = add_size(size, hash_estimate_size(1024,
											 sizeof(DMQDestinations)));
	return MAXALIGN(size);
}

static void
HOOK_shmem_startup(void)
{
	bool found;
	HASHCTL		hash_info;

	if (PreviousShmemStartupHook)
		(*PreviousShmemStartupHook)();

	MemSet(&hash_info, 0, sizeof(hash_info));
	hash_info.keysize = sizeof(Oid);
	hash_info.entrysize = sizeof(DMQDestinations);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	ExchShmem = ShmemInitStruct("exchange",
								sizeof(ExchangeSharedState),
								&found);
	if (!found)
		ExchShmem->lock = &(GetNamedLWLockTranche("exchange"))->lock;

	ExchShmem->htab = ShmemInitHash("dmq_destinations",
								10,
								1024,
								&hash_info,
								HASH_ELEM);
	LWLockRelease(AddinShmemInitLock);
}

void
EXEC_Hooks_init(void)
{
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = HOOK_Baserel_paths;
	prev_set_join_pathlist_hook = set_join_pathlist_hook;
	set_join_pathlist_hook = HOOK_Join_pathlist;
	prev_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = HOOK_Upper_paths;

	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = HOOK_shmem_startup;

	old_dmq_receiver_stop_hook = dmq_receiver_stop_hook;
	dmq_receiver_stop_hook = OnNodeDisconnect;

	DistributedPathsInit();
	RequestAddinShmemSpace(shmem_size());
	RequestNamedLWLockTranche("exchange", 1);

	memory_context = AllocSetContextCreate(TopMemoryContext,
							"EXCHANGE_MEMCONTEXT", ALLOCSET_DEFAULT_SIZES*8);
}
