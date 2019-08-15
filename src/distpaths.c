/*
 * distpaths.c
 * 		Distributed paths generator.
 *
 * Use DISPATCH and EXCHANGE custom nodes to create distributed paths as some
 * effective alternative to FDW paths. This paths allow to execute distributed
 * query in parallel manner: query execution stuff works at each instance
 * asynchronously and can redistribute tuples if it is needed.
 *
 * Copyright (c) 2018 - 2019, Postgres Professional
 *
 */

#include "postgres.h"
#include "catalog/pg_opclass.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "postgres_fdw.h"

#include "common.h"
#include "distpaths.h"
#include "exchange.h"
#include "nodeDistPlanExec.h"
#include "nodeDummyscan.h"
#include "partutils.h"


static Path * make_local_scan_path(Path *localPath, RelOptInfo *rel,
					 	 	 	 	 	 	 	 	 IndexOptInfo **indexinfo);
static Bitmapset *prepare_exchange(PlannerInfo *root, RelOptInfo *joinrel,
				RelOptInfo *orel, RelOptInfo *irel,
				CustomPath *opath, CustomPath *ipath,
				ExchangeMode omode, ExchangeMode imode,
				Distribution odist, Distribution idist);
static List *post_exchange(PlannerInfo *root, RelOptInfo *joinrel,
				RelOptInfo *orel, RelOptInfo *irel, const Bitmapset *servers);
static void arrange_partitions(List *restrictlist, Distribution *dist1,
				Distribution *dist2, Relids lrelids, Relids rrelids);
static bool have_equi_distribution(const Distribution dist1,
													const Distribution dist2);
static inline void reset_relpaths(RelOptInfo *rel);



/* Initialize custom nodes */
void
DistributedPathsInit(void)
{
	DISPATCH_Init_methods();
	EXCHANGE_Init_methods();
	DUMMYSCAN_Init_methods();
}

/*
 * Add distributed paths for a base relation scan: replace all ForeignScan nodes
 * by local Scan nodes.
 * Assumptions:
 * 1. If the planner chooses this type of scan for one partition of the relation,
 * then the same type of scan must be chosen for any other partition of this
 * relation.
 * 2. Type of scan chosen for local partition of a relation will be correct and
 * optimal for any foreign partition of the same relation.
 */
List *
distributedscan_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	List *distributed_pathlist = NIL;
	ListCell *lc;

	if (!rte->inh || rel->part_scheme == NULL)
	{
		/*
		 * Relation is not contain any partitions.
		 * At this level we don't need to generate distributed paths for such
		 * relation. But query can contain JOIN with partitioned relation.
		 * May be effective to broadcast local relation to all instances to JOIN.
		 * In this case we will try to add exchange path for this relation at
		 * the future.
		 */
		return NIL;
	}
Assert(0);
	/*
	 * We may need exchange paths only for the partitioned relations.
	 * In the case of scanning only one partition, planner will use FDW.
	 *
	 * Traverse all possible paths and search for APPEND
	 */
	foreach(lc, rel->pathlist)
	{
		Path		*path = (Path *) lfirst(lc);
		Path		*tmpLocalScanPath = NULL;
		AppendPath	*ap;
		ListCell	*lc1;
		Bitmapset	*servers = NULL;
		List		*subpaths = NIL;
		List		**append_paths;
		IndexOptInfo *indexinfo = NULL;

		/*
		 * In the case of partitioned relation all paths ends up by an Append
		 * or MergeAppend path node.
		 */
		switch (nodeTag(path))
		{
		case T_AppendPath:
			append_paths = &((AppendPath *) path)->subpaths;
			break;
		case T_MergeAppendPath:
			append_paths = &((MergeAppendPath *) path)->subpaths;
			break;
		default:
			elog(FATAL, "Unexpected node type %d, pathtype %d", path->type,
																path->pathtype);
		}

		/*
		 * Search for the first local scan node. We assume scan nodes of all
		 * relation partitions will have same type. It is caused by symmetry of
		 * data placement assumption.
		 */
		for (lc1 = list_head(*append_paths); lc1 != NULL; lc1 = lnext(lc1))
		{
			Path *subpath = (Path *) lfirst(lc1);

			if (subpath->pathtype == T_ForeignScan)
				continue;

			/*  Temporary suppress parametrized scan */
			if (PATH_REQ_OUTER(subpath))
				continue;

			tmpLocalScanPath = subpath;
			break;
		}

		if (!tmpLocalScanPath)
		{
			/*
			 * TODO: if all partitions placed at another instances.
			 * We do not have info about statistics and so on.
			 */
			continue;
		}

		/*
		 * Traverse all APPEND subpaths. Form new subpaths list. Collect remote
		 * servers.
		 */
		foreach(lc1, *append_paths)
		{
			Path	*subpath = (Path *) lfirst(lc1);
			Path	*tmpPath = NULL;
			Oid		serverid = InvalidOid;

			switch (nodeTag(subpath))
			{
			case T_Path:
			case T_BitmapHeapPath:
			case T_IndexPath:
			case T_TidPath:
			case T_SubqueryScanPath:
				tmpPath = subpath;
				serverid = InvalidOid;
				break;

			case T_ForeignPath:
			{
				PgFdwRelationInfo *fpinfo =
							(PgFdwRelationInfo *) subpath->parent->fdw_private;

				serverid = subpath->parent->serverid;
				tmpPath = make_local_scan_path(tmpLocalScanPath,
												subpath->parent, &indexinfo);
				Assert(subpath->parent->fdw_private != NULL);
				tmpPath->rows = fpinfo->rows;
				tmpPath->total_cost = tmpPath->rows/(tmpLocalScanPath->rows+1) *
												tmpLocalScanPath->total_cost;
				tmpPath->startup_cost = tmpLocalScanPath->startup_cost;
			}
				break;

			default:
				Assert(0);
				elog(FATAL, "Unexpected path node: %d", nodeTag(subpath));
			}

			Assert(tmpLocalScanPath);
			subpaths = lappend(subpaths, tmpPath);

			if (OidIsValid(serverid) && !bms_is_member((int)serverid, servers))
				servers = bms_add_member(servers, serverid);
		}

		if (servers == NULL)
			/* No one foreign servers were found. */
			return NIL;

		/*
		 * Create DISTEXEC->GATHER->APPEND node set.
		 */

		ap = create_append_path(root, rel, subpaths, NIL,
								PATH_REQ_OUTER(tmpLocalScanPath), 0, false,
								((AppendPath *) path)->partitioned_rels, -1);

		/*
		 * We assume that NULL value of distribution function means that we must
		 * use the partitioning scheme from the RelOptInfo struct.
		 */
		path = (Path *) create_exchange_path(root, rel, (Path *) ap,
															EXCH_GATHER, NULL);

		/*
		 * If scan path uses index we need to store it in the exchange node
		 * to use in localization procedure at another instances.
		 */
		if (indexinfo)
		{
			List **private;

			private = &((ExchangePath *) path)->cp.custom_private;
			*private = lappend(*private, indexinfo);
		}

		/* Add head of distributed plan */
		path = (Path *) create_distexec_path(root, rel, path, servers);

		distributed_pathlist = lappend(distributed_pathlist, path);
		bms_free(servers);
	}
	return distributed_pathlist;
}

List *
create_distributed_join_paths(PlannerInfo *root, RelOptInfo *joinrel,
						 RelOptInfo *outerrel, RelOptInfo *innerrel,
						 JoinType jointype, JoinPathExtraData *extra)
{
	List		*ipaths = innerrel->pathlist;
	List		*opaths = outerrel->pathlist;
	List		*pathlist = joinrel->pathlist;
	List		*paths = NIL;
	ListCell	*lc;

	foreach(lc, ipaths)
	{
		CustomPath	*ipath = (CustomPath *) lfirst(lc);
		ListCell	*lc1;

		if (!IsDispatcherNode(ipath))
			continue;
		Assert(0);
		foreach(lc1, opaths)
		{
			CustomPath		*opath = (CustomPath *) lfirst(lc1);
			RelOptInfo		orel,
							irel;
			List			*jpaths;
			Bitmapset		*servers = bms_add_member(NULL, InvalidOid);
			Distribution	odist;
			Distribution	idist;
			ExchangePath	*oep;
			ExchangePath	*iep;
			ExchangeMode	omode;
			ExchangeMode	imode;
			ListCell		*lc2;
			List *dellist = NIL;

			if (!IsDispatcherNode(opath))
				continue;

			memcpy(&orel, outerrel, sizeof(RelOptInfo));
			memcpy(&irel, innerrel, sizeof(RelOptInfo));

			if (joinrel->part_scheme == NULL)
				build_joinrel_partition_info(joinrel, &orel, &irel,
												extra->restrictlist, jointype);

			/*
			 * Simplistic alternative for FDW in the case without tuples shuffling.
			 */
			if (joinrel->part_scheme != NULL)
			{
				/*
				 * Do not need shuffle tuples at all. It is more optimal
				 * strategy and we do not need to try any more.
				 */
				servers = prepare_exchange(root, joinrel, &orel, &irel, opath,
								ipath, EXCH_STEALTH, EXCH_STEALTH, NULL, NULL);
				/* This call adds one JOIN path to the pathlist */
				add_paths_to_joinrel(root, joinrel, &orel, &irel, jointype,
											extra->sjinfo, extra->restrictlist);
				jpaths = post_exchange(root, joinrel, &orel, &irel, servers);
				paths = list_concat(paths, jpaths);
				continue;
			}

			/*
			 * Add basic exchange strategy
			 */
			servers = prepare_exchange(root, joinrel, &orel, &irel, opath, ipath,
									EXCH_STEALTH, EXCH_BROADCAST, NULL, NULL);
			/* This call adds one JOIN path to the pathlist */
			add_paths_to_joinrel(root, joinrel, &orel, &irel, jointype,
											extra->sjinfo, extra->restrictlist);
			jpaths = post_exchange(root, joinrel, &orel, &irel, servers);
			paths = list_concat(paths, jpaths);

			/*
			 * Add shuffle JOIN strategy, if needed
			 */
			arrange_partitions(extra->restrictlist, &odist, &idist,
													orel.relids, irel.relids);
			odist->servers = idist->servers = bms_copy(servers);
			odist->nparts = idist->nparts = bms_num_members(servers);

			oep = (ExchangePath *) cstmSubPath1(opath);
			iep = (ExchangePath *) cstmSubPath1(ipath);

			if (!oep->dist)
			{
				Assert(oep->mode == EXCH_GATHER);
				oep->dist = InitGatherDistribution(oep->cp.path.parent);
			}
			if (!iep->dist)
			{
				Assert(iep->mode == EXCH_GATHER);
				iep->dist = InitGatherDistribution(iep->cp.path.parent);
			}

			omode = have_equi_distribution(odist, oep->dist) ?
												EXCH_STEALTH : EXCH_SHUFFLE;
			imode = have_equi_distribution(idist, iep->dist) ?
													EXCH_STEALTH : EXCH_SHUFFLE;

			servers = prepare_exchange(root, joinrel, &orel, &irel, opath, ipath,
													omode, imode, odist, idist);
			/* Add JOIN paths to the pathlist */
			add_paths_to_joinrel(root, joinrel, &orel, &irel, jointype,
											extra->sjinfo, extra->restrictlist);
			foreach(lc2, joinrel->pathlist)
			{
				Path *path = (Path *) lfirst(lc2);

				if (path->pathtype == T_NestLoop)
					/* Do not build shuffle strategies for nested loops */
					dellist = lappend(dellist, path);
			}
			foreach(lc2, dellist)
				joinrel->pathlist = list_delete_ptr(joinrel->pathlist, lfirst(lc2));

			if (list_length(joinrel->pathlist) == 0)
				continue;

			jpaths = post_exchange(root, joinrel, &orel, &irel, servers);
			paths = list_concat(paths, jpaths);
		}
	}

	joinrel->pathlist = pathlist;
	return paths;
}

/*
 * FDW paths and EXCHANGE paths are incompatible and can't be combined at a plan.
 * We need to construct two non-intersecting path branches across all plan.
 * Costs of this plans is not an indicator of path quality at intermediate
 * stages of a plan building. We need bypass add_path() path cost checking
 * procedure.
 */
void
force_add_path(RelOptInfo *rel, List *pathlist)
{
	List		*prev_plist = rel->pathlist;
	List		*prev_pplist = rel->partial_pathlist;
	ListCell	*lc;

	/* Clear path lists and previous decision info */
	rel->pathlist = NIL;
	rel->partial_pathlist = NIL;
	rel->cheapest_parameterized_paths = NIL;
	rel->cheapest_startup_path = rel->cheapest_total_path =
											rel->cheapest_unique_path = NULL;

	/* Add new paths with cost filter. */
	foreach(lc, pathlist)
		add_path(rel, (Path *) lfirst(lc));

	/* Add previous path list */
	rel->pathlist = list_concat(rel->pathlist, prev_plist);
	rel->partial_pathlist = list_concat(rel->partial_pathlist, prev_pplist);

	/* Do the new decisions */
	set_cheapest(rel);
}

/*
 * Delete all bad paths and check on FDW paths existence.
 * It is paths with DISPATCH node at inner or outer subtrees.
 * We can't execute the plan because FDW has only one connection to one
 * foreign server and it is impossible to launch another query before end
 * of previous query.
 * TODO: keep in mind partial pathlist.
 */
void
remove_bad_paths(RelOptInfo *rel, bool *has_fdw_paths)
{
	ListCell	*lc;
	List		*bad_paths = NIL;

	Assert(has_fdw_paths != NULL);
	*has_fdw_paths = false;

	foreach(lc, rel->pathlist)
	{
		Path		*path = lfirst(lc);
		JoinPath	*jp = NULL;

		if (IsDispatcherNode(path))
			continue;

		/*
		 * Path head can be a ForeignScan node (after partition wise
		 * transformations. It is a good path.
		 */
		if (nodeTag(path) == T_NestPath || nodeTag(path) == T_MergePath ||
			nodeTag(path) == T_HashPath)
			jp = (JoinPath *) path;

		if (jp && (HasDispatcherNode(jp->innerjoinpath, NULL) ||
			HasDispatcherNode(jp->outerjoinpath, NULL)))
		{
			bad_paths = lappend(bad_paths, path);
			continue;
		}

		*has_fdw_paths = true;
	}

	/* Remove bas paths. */
	foreach(lc, bad_paths)
		rel->pathlist = list_delete_ptr(rel->pathlist, lfirst(lc));
	list_free(bad_paths);
}

/*
 * EXCHANGE paths can be more cheaper than FDW paths. In this case its fully
 * drive out FDW paths. We need to add FDW paths in force manner.
 */
void
create_fdw_paths(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
				 RelOptInfo *innerrel, JoinType jointype,
				 JoinPathExtraData *extra)
{
	ListCell	*lc;
	List		*ipathlist = NIL;
	List		*opathlist = NIL;

	foreach(lc, innerrel->pathlist)
	{
		CustomPath	*ipath = (CustomPath *) lfirst(lc);
		if (!IsDispatcherNode(ipath))
			continue;

		ipathlist = lappend(ipathlist, ipath);
	}

	foreach(lc, outerrel->pathlist)
	{
		CustomPath	*opath = (CustomPath *) lfirst(lc);
		if (!IsDispatcherNode(opath))
			continue;

		opathlist = lappend(opathlist, opath);
	}

	foreach(lc, ipathlist)
		innerrel->pathlist = list_delete_ptr(innerrel->pathlist, lfirst(lc));
	Assert(list_length(innerrel->pathlist) > 0);
	foreach(lc, opathlist)
		outerrel->pathlist = list_delete_ptr(outerrel->pathlist, lfirst(lc));
	Assert(list_length(outerrel->pathlist) > 0);

	joinrel->partial_pathlist = NIL;
	joinrel->cheapest_parameterized_paths = NIL;
	joinrel->cheapest_startup_path = joinrel->cheapest_total_path =
										joinrel->cheapest_unique_path = NULL;

	set_cheapest(innerrel);
	set_cheapest(outerrel);

	add_paths_to_joinrel(root, joinrel, outerrel, innerrel, jointype,
											extra->sjinfo, extra->restrictlist);

	Assert(joinrel->pathlist != NULL && list_length(joinrel->pathlist) > 0);

	innerrel->pathlist = list_concat(innerrel->pathlist, ipathlist);
	outerrel->pathlist = list_concat(outerrel->pathlist, opathlist);

	set_cheapest(innerrel);
	set_cheapest(outerrel);
	set_cheapest(joinrel);
}

/*
 * Make scan path for foreign part as for local relation with assumptions:
 * 1. Size and statistics of foreign part is simple as local part.
 * 2. Indexes, triggers and others on foreign part is same as on local part.
 */
static Path *
make_local_scan_path(Path *localPath, RelOptInfo *rel,
					 IndexOptInfo **indexinfo)
{
	Path *pathnode = NULL;

	switch (nodeTag(localPath))
	{
	case T_Path:
		Assert(localPath->pathtype == T_SeqScan);
		pathnode = makeNode(Path);
		memcpy(pathnode, localPath, sizeof(Path));
		pathnode->parent = rel;
		pathnode->pathtarget = rel->reltarget;
		break;

	case T_BitmapHeapPath:
	{
		BitmapHeapPath *path = makeNode(BitmapHeapPath);
		BitmapHeapPath *ptr = (BitmapHeapPath *) localPath;

		memcpy(path, localPath, sizeof(BitmapHeapPath));
		pathnode = &path->path;
		pathnode->parent = rel;
		pathnode->pathtarget = rel->reltarget;
		path->bitmapqual = make_local_scan_path(ptr->bitmapqual, rel, indexinfo);
	}
		break;

	case T_BitmapAndPath:
	{
		BitmapAndPath *path = makeNode(BitmapAndPath);
		BitmapAndPath *ptr = (BitmapAndPath *) localPath;
		ListCell *lc;

		memcpy(path, localPath, sizeof(BitmapAndPath));
		pathnode = &path->path;
		pathnode->parent = rel;
		pathnode->pathtarget = rel->reltarget;
		path->bitmapquals = NIL;
		foreach(lc, ptr->bitmapquals)
		{
			Path *subpath = (Path *)lfirst(lc);
			path->bitmapquals = lappend(path->bitmapquals,
						make_local_scan_path(subpath, rel, indexinfo));
		}
	}
		break;

	case T_BitmapOrPath:
	{
		BitmapOrPath *path = makeNode(BitmapOrPath);
		BitmapOrPath *ptr = (BitmapOrPath *) localPath;
		ListCell *lc;

		memcpy(path, localPath, sizeof(BitmapOrPath));
		pathnode = &path->path;
		pathnode->parent = rel;
		pathnode->pathtarget = rel->reltarget;
		path->bitmapquals = NIL;
		foreach(lc, ptr->bitmapquals)
		{
			Path *subpath = (Path *)lfirst(lc);
			path->bitmapquals = lappend(path->bitmapquals,
						make_local_scan_path(subpath, rel, indexinfo));
		}
	}
		break;

	case T_IndexPath:
	{
		IndexPath *path = makeNode(IndexPath);

		memcpy(path, localPath, sizeof(IndexPath));
		*indexinfo = path->indexinfo;
		pathnode = &path->path;
		pathnode->parent = rel;
		pathnode->pathtarget = rel->reltarget;
	}
		break;

	default:
		Assert(0);
	}

	return pathnode;
}

static Bitmapset *
prepare_exchange(PlannerInfo *root, RelOptInfo *joinrel,
				 RelOptInfo *orel, RelOptInfo *irel,
				 CustomPath *opath, CustomPath *ipath,
				 ExchangeMode omode, ExchangeMode imode,
				 Distribution odist, Distribution idist)
{
	ExchangePath	*oexch;
	ExchangePath	*iexch;
	ExchangePath	*ochild;
	ExchangePath	*ichild;
	Bitmapset		*servers;
	Distribution	dist;

	Assert(IsDispatcherNode(opath) && IsDispatcherNode(ipath));
	oexch = (ExchangePath *) cstmSubPath1(opath);
	iexch = (ExchangePath *) cstmSubPath1(ipath);
	Assert(IsExchangePathNode(oexch) && IsExchangePathNode(iexch));
	Assert(oexch->mode == EXCH_GATHER && iexch->mode == EXCH_GATHER);

	reset_relpaths(irel);
	reset_relpaths(orel);
	reset_relpaths(joinrel);

	/* Fill in a bitmap of involved foreign servers */
	servers = bms_add_member(NULL, InvalidOid);
	servers = bms_union(servers, extractForeignServers((CustomPath *) opath));
	servers = bms_union(servers, extractForeignServers((CustomPath *) ipath));

	/*
	 * Create new EXCHANGE nodes. Here we create new EXCHANGE node and need to
	 * pass distribution rules from the outer subtree. The distribution rules
	 * are stored at the field 'oexch->dist' of GATHER node.
	 * In the case of SHUFFLE node field 'odist' stores new distribution and
	 * we do not interested at the subtree distribution info.
	 */
	dist = (odist != NULL) ? odist :
						InitDistributionFunc(omode, oexch, NULL, NULL, servers);
	ochild = create_exchange_path(root, orel, cstmSubPath1(oexch), omode, dist);
	dist = (idist != NULL) ? idist :
						InitDistributionFunc(imode, iexch, NULL, NULL, servers);
	ichild = create_exchange_path(root, irel, cstmSubPath1(iexch), imode, dist);
	add_path(orel, (Path *) ochild);
	add_path(irel, (Path *) ichild);

	/* Set cheapest path before attempt of building JOIN path */
	set_cheapest(orel);
	set_cheapest(irel);

	return servers;
}

/*
 * Post-process of generated JOIN path.
 */
static List *
post_exchange(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *orel,
			  RelOptInfo *irel, const Bitmapset *servers)
{
	Path			*path;
	List			*paths = NIL;
	ExchangePath	*oexch;
	ExchangePath	*iexch;
	Distribution	dist;
	ListCell		*lc;

	/* Sanity checks */
	Assert(list_length(orel->pathlist) == 1 && list_length(irel->pathlist) == 1);
	oexch = linitial(orel->pathlist);
	iexch = linitial(irel->pathlist);
	Assert(IsExchangePathNode(oexch) && IsExchangePathNode(iexch));

	/* Set old parent value. We changed it before JOIN creation. */
	path = (Path *) linitial(oexch->cp.custom_paths);
	oexch->cp.path.parent = path->parent;
	path = (Path *) linitial(iexch->cp.custom_paths);
	iexch->cp.path.parent = path->parent;

	foreach(lc, joinrel->pathlist)
	{
		JoinPath	*jp = (JoinPath *) lfirst(lc);

		Assert(jp->path.type == T_NestPath || jp->path.type == T_HashPath ||
				jp->path.type == T_MergePath);

		/* Create the head of the JOIN path. */
		dist = InitDistributionFunc(EXCH_GATHER, oexch, iexch, jp, servers);
		path = (Path *) create_exchange_path(root, joinrel,
								linitial(joinrel->pathlist), EXCH_GATHER, dist);
		path = (Path *) create_distexec_path(root, joinrel, path, servers);
		paths = lappend(paths, path);
	}
	return paths;
}

#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_am.h"
#include "catalog/pg_opclass.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "partitioning/partbounds.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static void
arrange_partitions(List *restrictlist, Distribution *dist1, Distribution *dist2,
				   Relids lrelids, Relids rrelids)
{
	PartitionScheme	ps = palloc0(sizeof(PartitionSchemeData));
	ListCell		*lc;
	int16			len = list_length(restrictlist);
	Distribution	dist1n;
	Distribution	dist2n;

	/* Allocate memory for data structures */
	dist1n = palloc0(sizeof(DistributionData));
	dist2n = palloc0(sizeof(DistributionData));
	dist1n->partexprs = (List **) palloc0(sizeof(List *) * len);
	dist2n->partexprs = (List **) palloc0(sizeof(List *) * len);
	ps->partnatts = 0;
	ps->partopfamily = (Oid *) palloc(sizeof(Oid) * len);
	ps->partopcintype = (Oid *) palloc(sizeof(Oid) * len);
	ps->parttypbyval = (bool *) palloc(sizeof(bool) * len);
	ps->parttyplen = (int16 *) palloc(sizeof(int16) * len);
	ps->partsupfunc = (FmgrInfo *) palloc(sizeof(FmgrInfo) * len);
	ps->partcollation = (Oid *) palloc(sizeof(Oid) * len);

	foreach(lc, restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		OpExpr *opexpr = (OpExpr *) rinfo->clause;
		Expr *expr1;
		Expr *expr2;
		int16 partno = ps->partnatts;
		Oid partopclass;
		HeapTuple opclasstup;
		Form_pg_opclass opclassform;

		Assert(is_opclause(opexpr));
		/* Skip clauses which can not be used for a join. */
		if (!rinfo->can_join)
			continue;

		/* Match the operands to the relation. */
		if (bms_is_subset(rinfo->left_relids, lrelids) &&
			bms_is_subset(rinfo->right_relids, rrelids))
		{
			expr1 = linitial(opexpr->args);
			expr2 = lsecond(opexpr->args);
		}
		else if (bms_is_subset(rinfo->left_relids, rrelids) &&
				 bms_is_subset(rinfo->right_relids, lrelids))
		{
			expr1 = lsecond(opexpr->args);
			expr2 = linitial(opexpr->args);
		}
		else
			Assert(0);

		dist1n->partexprs[partno] = lappend(dist1n->partexprs[partno], copyObject(expr1));
		dist2n->partexprs[partno] = lappend(dist2n->partexprs[partno], copyObject(expr2));

		ps->partcollation[partno] = exprCollation((Node *) expr1);
		Assert(exprCollation((Node *) expr1) == exprCollation((Node *) expr2));
		Assert(exprType((Node *) expr1) == exprType((Node *) expr2));
		partopclass = GetDefaultOpClass(exprType((Node *) expr1), HASH_AM_OID);
		opclasstup = SearchSysCache1(CLAOID, ObjectIdGetDatum(partopclass));
		if (!HeapTupleIsValid(opclasstup))
			elog(ERROR, "cache lookup failed for partclass %u", partopclass);
		opclassform = (Form_pg_opclass) GETSTRUCT(opclasstup);
		ps->partopfamily[partno] = opclassform->opcfamily;
		ps->partopcintype[partno] = opclassform->opcintype;
		ps->partnatts++;
		ReleaseSysCache(opclasstup);
	}

	ps->strategy = PARTITION_STRATEGY_HASH;
	dist2n->part_scheme = dist1n->part_scheme = ps;
	*dist1 = dist1n;
	*dist2 = dist2n;
}

static int
match_expr_to_partition_keys(Expr *expr, Distribution dist)
{
	int			cnt;

	/* This function should be called only for partitioned relations. */
	Assert(dist->part_scheme);

	/* Remove any relabel decorations. */
	while (IsA(expr, RelabelType))
		expr = (Expr *) (castNode(RelabelType, expr))->arg;

	for (cnt = 0; cnt < dist->part_scheme->partnatts; cnt++)
	{
		ListCell   *lc;

		Assert(dist->partexprs);
		foreach(lc, dist->partexprs[cnt])
		{
			if (equal(lfirst(lc), expr))
				return cnt;
		}
	}
	/* TODO: nullable_partexprs */

	return -1;
}

static bool
have_equi_distribution(const Distribution dist1, const Distribution dist2)
{
	PartitionScheme ps = dist2->part_scheme;
	ListCell   *lc;
	int partno;

	/*
	 * TODO:
	Assert(rel1->part_scheme == rel2->part_scheme); */

	if (!bms_equal(dist1->servers, dist2->servers))
		return false;

	for (partno = 0; partno < ps->partnatts; partno++)
	{
		foreach(lc, dist2->partexprs[partno])
		{
			Expr	*expr = lfirst(lc);

			if (match_expr_to_partition_keys(expr, dist1) != partno)
				return false;
		}
	}

	return true;
}

static inline void
reset_relpaths(RelOptInfo *rel)
{
	rel->cheapest_parameterized_paths = NIL;
	rel->cheapest_startup_path = rel->cheapest_total_path =
	rel->cheapest_unique_path = NULL;
	rel->pathlist = NIL;
	rel->partial_pathlist = NIL;
}
