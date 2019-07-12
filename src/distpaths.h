/*
 * distpaths.h
 *
 */

#ifndef DISTPATHS_H_
#define DISTPATHS_H_


#include "postgres.h"
#include "nodes/relation.h"

extern void DistributedPathsInit(void);
extern List * distributedscan_pathlist(PlannerInfo *root, RelOptInfo *rel,
															RangeTblEntry *rte);
extern List *create_distributed_join_paths(PlannerInfo *root,
						RelOptInfo *joinrel,
						RelOptInfo *outerrel, RelOptInfo *innerrel,
						JoinType jointype, JoinPathExtraData *extra);
extern void force_add_path(RelOptInfo *rel, List *pathlist);
extern void remove_bad_paths(RelOptInfo *rel, bool *has_fdw_paths);
extern void create_fdw_paths(PlannerInfo *root, RelOptInfo *joinrel,
		 RelOptInfo *outerrel, RelOptInfo *innerrel,
		 JoinType jointype, JoinPathExtraData *extra);
#endif /* DISTPATHS_H_ */
