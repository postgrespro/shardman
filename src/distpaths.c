/*
 * distpaths.c
 *
 */

#include "postgres.h"
#include "catalog/pg_opclass.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
//#include "partitioning/partbounds.h"
#include "postgres_fdw.h"

#include "common.h"
#include "distpaths.h"
#include "exchange.h"
#include "nodeDistPlanExec.h"
#include "nodeDummyscan.h"
#include "partutils.h"


static Path * make_local_scan_path(Path *localPath, RelOptInfo *rel,
					 	 	 	 	 	 	 	 	 IndexOptInfo **indexinfo);

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

/*
 * Add one path for a base relation target:  replace all ForeignScan nodes by
 * local Scan nodes.
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

	if (!rte->inh)
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

	/* Set distribution data for base relation. */
	if (rel->part_scheme == NULL)
	{
		elog(WARNING, "Relation %d has no partitions", rel->relid);
		Assert(0);
	}

	/*
	 * We may need exchange paths only for the partitioned relations.
	 * In the case of scanning only one partition, planner will use FDW.
	 *
	 * Traverse all possible paths and search for APPEND */
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

			tmpLocalScanPath = subpath;
			break;
		}

		if (!tmpLocalScanPath)
		{
			/*
			 * TODO: if all partitions placed at another instances.
			 * We do not have info about statistics and so on.
			 */
			elog(WARNING, "All selected partitions are placed at another nodes. Ignore distribution planning.");
			return NIL;
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
				tmpPath->total_cost += fpinfo->total_cost - fpinfo->startup_cost;
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

void
DistributedPathsInit(void)
{
	EXCHANGE_Init_methods();
	DUMMYSCAN_Init_methods();
}


#include "nodes/print.h"
static void
print_relids(PlannerInfo *root, Relids relids)
{
	int			x;
	bool		first = true;

	x = -1;
	while ((x = bms_next_member(relids, x)) >= 0)
	{
		if (!first)
			printf(" ");
		if (x < root->simple_rel_array_size &&
			root->simple_rte_array[x])
			printf("%s", root->simple_rte_array[x]->eref->aliasname);
		else
			printf("%d", x);
		first = false;
	}
}

static void
print_restrictclauses(PlannerInfo *root, List *clauses)
{
	ListCell   *l;

	foreach(l, clauses)
	{
		RestrictInfo *c = lfirst(l);

		print_expr((Node *) c->clause, root->parse->rtable);
		if (lnext(l))
			printf(", ");
	}
}

static void
print_path(PlannerInfo *root, Path *path, int indent)
{
	const char *ptype;
	bool		join = false;
	Path	   *subpath = NULL;
	int			i;

	switch (nodeTag(path))
	{
		case T_Path:
			switch (path->pathtype)
			{
				case T_SeqScan:
					ptype = "SeqScan";
					break;
				case T_SampleScan:
					ptype = "SampleScan";
					break;
				case T_SubqueryScan:
					ptype = "SubqueryScan";
					break;
				case T_FunctionScan:
					ptype = "FunctionScan";
					break;
				case T_TableFuncScan:
					ptype = "TableFuncScan";
					break;
				case T_ValuesScan:
					ptype = "ValuesScan";
					break;
				case T_CteScan:
					ptype = "CteScan";
					break;
				case T_WorkTableScan:
					ptype = "WorkTableScan";
					break;
				default:
					ptype = "???Path";
					break;
			}
			break;
		case T_IndexPath:
			ptype = "IdxScan";
			break;
		case T_BitmapHeapPath:
			ptype = "BitmapHeapScan";
			break;
		case T_BitmapAndPath:
			ptype = "BitmapAndPath";
			break;
		case T_BitmapOrPath:
			ptype = "BitmapOrPath";
			break;
		case T_TidPath:
			ptype = "TidScan";
			break;
		case T_SubqueryScanPath:
			ptype = "SubqueryScanScan";
			break;
		case T_ForeignPath:
			ptype = "ForeignScan";
			break;
		case T_CustomPath:
		{
			static char str[1024];

			if (IsExchangePathNode(path))
			{
				ExchangePath *epath = (ExchangePath *) path;

				snprintf(str,  1024, "CustomScan - EXCHANGE mode=%d", epath->mode);
				ptype = str;
			}
			else
				ptype = "CustomScan";
		}
			break;
		case T_NestPath:
			ptype = "NestLoop";
			join = true;
			break;
		case T_MergePath:
			ptype = "MergeJoin";
			join = true;
			break;
		case T_HashPath:
			ptype = "HashJoin";
			join = true;
			break;
		case T_AppendPath:
			ptype = "Append";
			break;
		case T_MergeAppendPath:
			ptype = "MergeAppend";
			break;
		case T_ResultPath:
			ptype = "Result";
			break;
		case T_MaterialPath:
			ptype = "Material";
			subpath = ((MaterialPath *) path)->subpath;
			break;
		case T_UniquePath:
			ptype = "Unique";
			subpath = ((UniquePath *) path)->subpath;
			break;
		case T_GatherPath:
			ptype = "Gather";
			subpath = ((GatherPath *) path)->subpath;
			break;
		case T_GatherMergePath:
			ptype = "GatherMerge";
			subpath = ((GatherMergePath *) path)->subpath;
			break;
		case T_ProjectionPath:
			ptype = "Projection";
			subpath = ((ProjectionPath *) path)->subpath;
			break;
		case T_ProjectSetPath:
			ptype = "ProjectSet";
			subpath = ((ProjectSetPath *) path)->subpath;
			break;
		case T_SortPath:
			ptype = "Sort";
			subpath = ((SortPath *) path)->subpath;
			break;
		case T_GroupPath:
			ptype = "Group";
			subpath = ((GroupPath *) path)->subpath;
			break;
		case T_UpperUniquePath:
			ptype = "UpperUnique";
			subpath = ((UpperUniquePath *) path)->subpath;
			break;
		case T_AggPath:
			ptype = "Agg";
			subpath = ((AggPath *) path)->subpath;
			break;
		case T_GroupingSetsPath:
			ptype = "GroupingSets";
			subpath = ((GroupingSetsPath *) path)->subpath;
			break;
		case T_MinMaxAggPath:
			ptype = "MinMaxAgg";
			break;
		case T_WindowAggPath:
			ptype = "WindowAgg";
			subpath = ((WindowAggPath *) path)->subpath;
			break;
		case T_SetOpPath:
			ptype = "SetOp";
			subpath = ((SetOpPath *) path)->subpath;
			break;
		case T_RecursiveUnionPath:
			ptype = "RecursiveUnion";
			break;
		case T_LockRowsPath:
			ptype = "LockRows";
			subpath = ((LockRowsPath *) path)->subpath;
			break;
		case T_ModifyTablePath:
			ptype = "ModifyTable";
			break;
		case T_LimitPath:
			ptype = "Limit";
			subpath = ((LimitPath *) path)->subpath;
			break;
		default:
			ptype = "???Path";
			break;
	}

	for (i = 0; i < indent; i++)
		printf("\t");
	printf("%s", ptype);

	if (path->parent)
	{
		printf("(");
		print_relids(root, path->parent->relids);
		printf(")");
	}
	if (path->param_info)
	{
		printf(" required_outer (");
		print_relids(root, path->param_info->ppi_req_outer);
		printf(")");
	}
	printf(" rows=%.0f cost=%.2f..%.2f\n",
		   path->rows, path->startup_cost, path->total_cost);

	if (path->pathkeys)
	{
		for (i = 0; i < indent; i++)
			printf("\t");
		printf("  pathkeys: ");
		print_pathkeys(path->pathkeys, root->parse->rtable);
	}

	if (join)
	{
		JoinPath   *jp = (JoinPath *) path;

		for (i = 0; i < indent; i++)
			printf("\t");
		printf("  clauses: ");
		print_restrictclauses(root, jp->joinrestrictinfo);
		printf("\n");

		if (IsA(path, MergePath))
		{
			MergePath  *mp = (MergePath *) path;

			for (i = 0; i < indent; i++)
				printf("\t");
			printf("  sortouter=%d sortinner=%d materializeinner=%d\n",
				   ((mp->outersortkeys) ? 1 : 0),
				   ((mp->innersortkeys) ? 1 : 0),
				   ((mp->materialize_inner) ? 1 : 0));
		}

		print_path(root, jp->outerjoinpath, indent + 1);
		print_path(root, jp->innerjoinpath, indent + 1);
	}

	if (subpath)
		print_path(root, subpath, indent + 1);

	if (nodeTag(path) == T_CustomPath)
	{
		ListCell *lc;
		foreach(lc, ((CustomPath *) path)->custom_paths)
		{
			subpath = (Path *) lfirst(lc);
			print_path(root, subpath, indent + 1);
		}
	}

	if (nodeTag(path) == T_AppendPath || nodeTag(path) == T_MergeAppendPath)
	{
		ListCell *lc;
		foreach(lc, ((AppendPath *) path)->subpaths)
		{
			subpath = (Path *) lfirst(lc);
			print_path(root, subpath, indent + 1);
		}
	}

	if (nodeTag(path) == T_ModifyTablePath)
	{
		ListCell *lc;
		foreach(lc, ((ModifyTablePath *) path)->subpaths)
		{
			subpath = (Path *) lfirst(lc);
			print_path(root, subpath, indent + 1);
		}
	}
}

void
debug_print_rel(PlannerInfo *root, RelOptInfo *rel)
{
	ListCell   *l;

	printf("RELOPTINFO (");
	print_relids(root, rel->relids);
	printf("): rows=%.0f width=%d\n", rel->rows, rel->reltarget->width);

	if (rel->baserestrictinfo)
	{
		printf("\tbaserestrictinfo: ");
		print_restrictclauses(root, rel->baserestrictinfo);
		printf("\n");
	}

	if (rel->joininfo)
	{
		printf("\tjoininfo: ");
		print_restrictclauses(root, rel->joininfo);
		printf("\n");
	}

	printf("\tpath list:\n");
	foreach(l, rel->pathlist)
		print_path(root, lfirst(l), 1);
	if (rel->cheapest_parameterized_paths)
	{
		printf("\n\tcheapest parameterized paths:\n");
		foreach(l, rel->cheapest_parameterized_paths)
			print_path(root, lfirst(l), 1);
	}
	if (rel->cheapest_startup_path)
	{
		printf("\n\tcheapest startup path:\n");
		print_path(root, rel->cheapest_startup_path, 1);
	}
	if (rel->cheapest_total_path)
	{
		printf("\n\tcheapest total path:\n");
		print_path(root, rel->cheapest_total_path, 1);
	}
	printf("\n");
	fflush(stdout);
}


/*
 * Distributed JOIN code
 */

static void
reset_relpaths(RelOptInfo *rel)
{
	rel->cheapest_parameterized_paths = NIL;
	rel->cheapest_startup_path = rel->cheapest_total_path =
	rel->cheapest_unique_path = NULL;
	rel->pathlist = NIL;
	rel->partial_pathlist = NIL;
}

static Distribution
InitDistributionFunc(ExchangeMode mode, const ExchangePath *oexch,
					 const ExchangePath *iexch, RelOptInfo *joinrel,
					 const Bitmapset *servers)
{
	Distribution dist = NULL;
	bool IsPartDist = false;

	/* Sanity check */
	Assert(oexch && IsExchangePathNode(oexch));
	Assert(servers && bms_num_members(servers) > 0);

	/* Default partitioning has changed by exchange operations? */
	if (oexch->dist == NULL)
		IsPartDist = true;

	switch (mode)
	{
	case EXCH_GATHER:
		if (IsPartDist)
			dist = InitDistribution(oexch->cp.path.parent);
		else
		{
			/* XXX: Bad code!!! */
			dist = oexch->dist;
		}

		break;

	case EXCH_STEALTH:
		Assert(iexch == NULL && joinrel == NULL);
		if (IsPartDist)
			dist = InitStealthDistribution(oexch->cp.path.parent, servers);
		else
		{
			/* Keep the incoming distribution. */
			dist = oexch->dist;
		}
		break;

	case EXCH_BROADCAST:
		Assert(iexch == NULL && joinrel == NULL);
		/* Eliminate distribution coming from the subtree */
		dist = InitBCastDistribution(servers);
		break;

	case EXCH_SHUFFLE:
		Assert(iexch == NULL);
		break;

	default:
		Assert(0);
	}

	return dist;
}

static Bitmapset *
prepare_exchange(PlannerInfo *root, RelOptInfo *joinrel,
				 RelOptInfo *orel, RelOptInfo *irel,
				 CustomPath *opath, CustomPath *ipath,
				 ExchangeMode omode, ExchangeMode imode)
{
	ExchangePath	*oexch;
	ExchangePath	*iexch;
	ExchangePath	*ochild;
	ExchangePath	*ichild;
	Bitmapset		*servers;
	Distribution	dist;

	Assert(IsDistExecNode(opath) && IsDistExecNode(ipath));
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

	/* Create new EXCHANGE nodes */
	dist = InitDistributionFunc(omode, oexch, NULL, NULL, servers);
	ochild = create_exchange_path(root, orel, cstmSubPath1(oexch), omode, dist);
	dist = InitDistributionFunc(imode, iexch, NULL, NULL, servers);
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
static Path *
post_exchange(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *orel,
			  RelOptInfo *irel, const Bitmapset *servers)
{
	Path			*path;
	ExchangePath	*oexch;
	ExchangePath	*iexch;
	Distribution	dist;

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

	/* Create the head of the JOIN path. */
	dist = InitDistributionFunc(EXCH_GATHER, oexch, iexch, joinrel, servers);
	path = (Path *) create_exchange_path(root, joinrel,
								linitial(joinrel->pathlist), EXCH_GATHER, dist);
	path = (Path *) create_distexec_path(root, joinrel, path, servers);
	return path;
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
arrange_partitions(RelOptInfo *rel1, RelOptInfo *rel2, List *restrictlist,
				   Distribution dist1, Distribution dist2)
{
	PartitionScheme part_scheme = palloc(sizeof(PartitionSchemeData));
	Bitmapset *servers = NULL;
	ListCell *lc;
	int16 len = list_length(restrictlist);
	int i;

	dist1->partexprs = (List **) palloc0(sizeof(List *) * len);
	dist2->partexprs = (List **) palloc0(sizeof(List *) * len);
	part_scheme->partnatts = 0;
	part_scheme->partopfamily = (Oid *) palloc(sizeof(Oid) * len);
	part_scheme->partopcintype = (Oid *) palloc(sizeof(Oid) * len);
	part_scheme->parttypbyval = (bool *) palloc(sizeof(bool) * len);
	part_scheme->parttyplen = (int16 *) palloc(sizeof(int16) * len);
	part_scheme->partsupfunc = (FmgrInfo *) palloc(sizeof(FmgrInfo) * len);
	part_scheme->partcollation = (Oid *) palloc(sizeof(Oid) * len);

	foreach(lc, restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		OpExpr *opexpr = (OpExpr *) rinfo->clause;
		Expr *expr1;
		Expr *expr2;
		int16 partno = part_scheme->partnatts;
		Oid partopclass;
		HeapTuple opclasstup;
		Form_pg_opclass opclassform;

		Assert(is_opclause(opexpr));
		/* Skip clauses which can not be used for a join. */
		if (!rinfo->can_join)
			continue;

		/* Match the operands to the relation. */
		if (bms_is_subset(rinfo->left_relids, rel1->relids) &&
			bms_is_subset(rinfo->right_relids, rel2->relids))
		{
			expr1 = linitial(opexpr->args);
			expr2 = lsecond(opexpr->args);
		}
		else if (bms_is_subset(rinfo->left_relids, rel2->relids) &&
				 bms_is_subset(rinfo->right_relids, rel1->relids))
		{
			expr1 = lsecond(opexpr->args);
			expr2 = linitial(opexpr->args);
		}
		else
			Assert(0);

		dist1->partexprs[partno] = lappend(dist1->partexprs[partno], copyObject(expr1));
		dist2->partexprs[partno] = lappend(dist2->partexprs[partno], copyObject(expr2));

		part_scheme->partcollation[partno] = exprCollation((Node *) expr1);
		Assert(exprCollation((Node *) expr1) == exprCollation((Node *) expr2));
		Assert(exprType((Node *) expr1) == exprType((Node *) expr2));
		partopclass = GetDefaultOpClass(exprType((Node *) expr1), HASH_AM_OID);
		opclasstup = SearchSysCache1(CLAOID, ObjectIdGetDatum(partopclass));
		if (!HeapTupleIsValid(opclasstup))
			elog(ERROR, "cache lookup failed for partclass %u", partopclass);
		opclassform = (Form_pg_opclass) GETSTRUCT(opclasstup);
		part_scheme->partopfamily[partno] = opclassform->opcfamily;
		part_scheme->partopcintype[partno] = opclassform->opcintype;
		part_scheme->partnatts++;
		ReleaseSysCache(opclasstup);
	}

	part_scheme->strategy = PARTITION_STRATEGY_HASH;
	dist1->part_scheme = dist2->part_scheme = part_scheme;

	for (i = 0; i < rel1->nparts; i++)
		if (OidIsValid(rel1->part_rels[i]->serverid) &&
					!bms_is_member((int) rel1->part_rels[i]->serverid, servers))
			servers = bms_add_member(servers, (int) rel1->part_rels[i]->serverid);
		else if (!bms_is_member(0, servers))
			servers = bms_add_member(servers, 0);

	for (i = 0; i < rel2->nparts; i++)
		if (OidIsValid(rel2->part_rels[i]->serverid) &&
					!bms_is_member((int) rel2->part_rels[i]->serverid, servers))
			servers = bms_add_member(servers, (int) rel2->part_rels[i]->serverid);
		else if (!bms_is_member(0, servers))
			servers = bms_add_member(servers, 0);
	dist1->nparts = dist2->nparts = bms_num_members(servers);
}

List *
create_distributed_join_paths(PlannerInfo *root, RelOptInfo *joinrel,
						 RelOptInfo *outerrel, RelOptInfo *innerrel,
						 JoinType jointype, JoinPathExtraData *extra)
{
	List		*ipaths = innerrel->pathlist;
	List		*opaths = outerrel->pathlist;
	List		*pathlist = joinrel->pathlist;
//	List		*partial_pathlist = joinrel->partial_pathlist;
	List		*paths = NIL;
	ListCell	*lc;

	foreach(lc, ipaths)
	{
		CustomPath	*ipath = (CustomPath *) lfirst(lc);
		ListCell	*lc1;

		if (!IsDistExecNode(ipath))
			continue;

		foreach(lc1, opaths)
		{
			CustomPath	*opath = (CustomPath *) lfirst(lc1);
			RelOptInfo	orel,
						irel;
			Path		*path;
			Bitmapset	*servers = bms_add_member(NULL, InvalidOid);
			Distribution dist1 = palloc0(sizeof(DistributionData));
			Distribution dist2 = palloc0(sizeof(DistributionData));

			if (!IsDistExecNode(opath))
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
											ipath, EXCH_STEALTH, EXCH_STEALTH);
				/* This call adds one JOIN path to the pathlist */
				add_paths_to_joinrel(root, joinrel, &orel, &irel, jointype,
											extra->sjinfo, extra->restrictlist);
				path = post_exchange(root, joinrel, &orel, &irel, servers);
				paths = lappend(paths, path);
				continue;
			}

			/* Add basic exchange strategy */
			servers = prepare_exchange(root, joinrel, &orel, &irel, opath, ipath,
												EXCH_STEALTH, EXCH_BROADCAST);
			/* This call adds one JOIN path to the pathlist */
			add_paths_to_joinrel(root, joinrel, &orel, &irel, jointype,
											extra->sjinfo, extra->restrictlist);
			path = post_exchange(root, joinrel, &orel, &irel, servers);
			paths = lappend(paths, path);

			if (path->pathtype == T_NestLoop)
				/* Do not build shuffle strategies for nested loops */
				continue;

			/* Add shuffle JOIN strategy, if needed */
			arrange_partitions(&orel, &irel, extra->restrictlist, dist1, dist2);
		}
	}

	joinrel->pathlist = pathlist;
	return paths;
}
