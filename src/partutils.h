/*
 * partutils.h
 *
 */

#ifndef PARTUTILS_H_
#define PARTUTILS_H_

#include "postgres.h"

#include "nodes/relation.h"

/*
 * Planner info about relation distribution
 */
typedef struct
{
	int				nparts;
	PartitionScheme	part_scheme;
	List			**partexprs;
	Bitmapset		*servers;
} DistributionData;

typedef DistributionData *Distribution;

extern Distribution InitGatherDistribution(RelOptInfo *rel);
extern Distribution InitStealthDistribution(RelOptInfo *rel);
extern Distribution InitBCastDistribution(const Bitmapset *servers);
extern bool build_joinrel_partition_info(RelOptInfo *joinrel,
										 RelOptInfo *outer_rel,
										 RelOptInfo *inner_rel,
										 List *restrictlist,
										 JoinType jointype);
extern Distribution build_joinrel_distributionFn(const Distribution odist,
												 const Distribution idist,
												 JoinType jointype);

#endif /* PARTUTILS_H_ */
