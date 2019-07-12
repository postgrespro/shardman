/* ------------------------------------------------------------------------
 *
 * hooks.c
 *		Hooks for distributed paths generator.
 *
 * Copyright (c) 2018 - 2019, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "postgres.h"
#include "optimizer/paths.h" /* Need for paths hooks */
#include "storage/ipc.h" /* shmem hook */
#include "utils/memutils.h"
#include "common.h"
#include "distpaths.h"
#include "hooks.h"


static set_rel_pathlist_hook_type	prev_set_rel_pathlist_hook = NULL;
static shmem_startup_hook_type		PreviousShmemStartupHook = NULL;
static set_join_pathlist_hook_type	prev_set_join_pathlist_hook = NULL;
static dmq_receiver_hook_type		old_dmq_receiver_stop_hook = NULL;

static bool joinpath_gen_recursion = false;


static void HOOK_Baserel_paths(PlannerInfo *root, RelOptInfo *rel, Index rti,
															RangeTblEntry *rte);
static void HOOK_Join_pathlist(PlannerInfo *root, RelOptInfo *joinrel,
							   RelOptInfo *outerrel, RelOptInfo *innerrel,
							   JoinType jointype, JoinPathExtraData *extra);
static Size shmem_size(void);
static void HOOK_shmem_startup(void);


/*
 * Set hooks for distributed paths generator. Initialize shared memory
 * segment and private memory context.
 */
void
Hooks_init(void)
{
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = HOOK_Baserel_paths;
	prev_set_join_pathlist_hook = set_join_pathlist_hook;
	set_join_pathlist_hook = HOOK_Join_pathlist;

	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = HOOK_shmem_startup;

	old_dmq_receiver_stop_hook = dmq_receiver_stop_hook;
	dmq_receiver_stop_hook = OnNodeDisconnect;

	DistributedPathsInit();
	RequestAddinShmemSpace(shmem_size());
	RequestNamedLWLockTranche("exchange", 1);

	DPGMemoryContext = AllocSetContextCreate(TopMemoryContext,
							"EXCHANGE_MEMCONTEXT", ALLOCSET_DEFAULT_SIZES*8);
}

/*
 * Make distributed paths for scan operation of partitioned relation.
 * Restrictions:
 * 1. At least one partition must be placed in the coordinator's storage.
 * 2. FDW and EXCHANGE-based plans do not mixed because uses only one connection
 * to each foreign server. Add EXCHANGE paths in non-concurrent manner to the
 * path list of base relation.
 */
static void
HOOK_Baserel_paths(PlannerInfo *root, RelOptInfo *rel, Index rti,
															RangeTblEntry *rte)
{
	List	*pathlist = NIL;

	if (prev_set_rel_pathlist_hook)
		(*prev_set_rel_pathlist_hook) (root, rel, rti, rte);

	/* Not create distributed paths if the GUC is disabled */
	if (!enable_distributed_execution)
		return;

	/* Get the list of distributed paths.*/
	pathlist = distributedscan_pathlist(root, rel, rte);

	/*
	 * Insert distributed paths into the relation pathlist.
	 * Hide FDW paths before addition.
	 */
	force_add_path(rel, pathlist);
	list_free(pathlist);
}

/*
 * If left and right relations are partitioned (TODO: not only) we can use an
 * exchange node as left or right son for tuples shuffling of a relation in
 * accordance with partitioning scheme of another relation.
 * As for base relation, EXCHANGE paths not concurrent with FDW paths. At least
 * one FDW path must be placed at the path list.
 */
static void
HOOK_Join_pathlist(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
		 	 	   RelOptInfo *innerrel, JoinType jointype,
				   JoinPathExtraData *extra)
{
	List	*paths;
	bool	has_fdw_paths;

	if (joinpath_gen_recursion)
		/*
		 * In the case of add_paths_to_joinrel() routine calling we will catch
		 * an recursion in the hook. Suppress this.
		 */
		return;
	joinpath_gen_recursion = true;

	if (prev_set_join_pathlist_hook)
			(*prev_set_join_pathlist_hook)(root, joinrel, outerrel, innerrel,
															jointype, extra);

	remove_bad_paths(joinrel, &has_fdw_paths);

	if (!has_fdw_paths)
		create_fdw_paths(root, joinrel, outerrel, innerrel, jointype, extra);

	paths = create_distributed_join_paths(root, joinrel, outerrel, innerrel,
															jointype, extra);

	force_add_path(joinrel, paths);
	list_free(paths);
	joinpath_gen_recursion = false;
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

	DPGShmem = ShmemInitStruct("exchange",
								sizeof(ExchangeSharedState),
								&found);
	if (!found)
	{
		/* Newly created shared memory */
		DPGShmem->lock = &(GetNamedLWLockTranche("exchange"))->lock;
		pg_atomic_init_u64(&DPGShmem->exchangeID, 0);
	}

	DPGShmem->htab = ShmemInitHash("dmq_destinations",
								10,
								1024,
								&hash_info,
								HASH_ELEM);
	LWLockRelease(AddinShmemInitLock);
}
