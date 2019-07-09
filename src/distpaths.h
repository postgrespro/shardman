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
extern void debug_print_rel(PlannerInfo *root, RelOptInfo *rel);

#endif /* DISTPATHS_H_ */
