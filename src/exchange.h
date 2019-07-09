/*-------------------------------------------------------------------------
 *
 * exchange.h
 *	Special custom node for tuples shuffling during parallel query execution.
 *
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * Author: Andrey Lepikhov <a.lepikhov@postgrespro.ru>
 *
 * IDENTIFICATION
 *	contrib/pargres/exchange.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXCHANGE_H_
#define EXCHANGE_H_

#include "nodes/extensible.h"
#include "optimizer/planner.h"

#include "common.h"
#include "dmq.h"
#include "partutils.h"


#define EXCHANGE_STATE_NAME		"ExchangeStateNode"
#define EXCHANGE_PATH_NAME		"ExchangePathNode"
#define EXCHANGE_PLAN_NAME		"ExchangePlanNode"
#define EXCHANGE_PRIVATE_NAME	"ExchangePlanPrivate"

#define IsExchangePathNode(node) \
	(IsA(node, CustomPath) && \
	(strcmp(((CustomPath *)(node))->methods->CustomName, \
			EXCHANGE_PATH_NAME) == 0))

#define IsExchangePlanNode(node) \
	(IsA(node, CustomScan) && \
	(strcmp(((CustomScan *)(node))->methods->CustomName, \
	EXCHANGE_PLAN_NAME) == 0))

#define IsExchangeStateNode(node) \
	(IsA(node, CustomScanState) && \
	(strcmp(((CustomScanState *)(node))->methods->CustomName, \
	EXCHANGE_STATE_NAME) == 0))


#define cstmSubPath1(customPath) (Path *) linitial(((CustomPath *) \
									customPath)->custom_paths)

#define cstmSubPlan1(custom) ((Plan *) linitial(((CustomScan *) \
									custom)->custom_plans))

typedef enum ExchangeMode
{
	EXCH_GATHER,
	EXCH_STEALTH,
	EXCH_SHUFFLE,
	EXCH_BROADCAST
} ExchangeMode;

/* Exchange Private Partitioning data */
typedef struct EPPNode
{
	ExtensibleNode node;

	int nnodes;
	NodeName *nodes;

	int16 natts;
	Oid *funcid;
	List *exprs;
	ExchangeMode mode;
} EPPNode;

typedef struct ExchangeState
{
	CustomScanState	css;
	char stream[256];
	DMQDestCont	*dests;
	bool init;
	EState *estate;
	bool hasLocal;
	int activeRemotes;
	int ltuples;
	int rtuples;
	int stuples;

	/* Partitioning info */
	int greatest_modulus;
	int16 partnatts;
	FmgrInfo *partsupfunc;
	List *partexprs;
	List *keystate;
	int nnodes; /* number of instances containing partitions */
	NodeName *nodes; /* Unique signature of each instance */
	int *indexes;
	ExchangeMode mode;
	IndexOptInfo *indexinfo;
} ExchangeState;

extern uint32 exchange_counter;

/*
 * Structure for private path data. It is used at paths generating step only.
 */
typedef struct ExchangePath
{
	CustomPath cp;

	ExchangeMode mode; /* It will send all tuples to a coordinator only. */
	/*
	 * Source data for a tuples distribution function building.
	 * Assume the NULL value means that the distribution corresponds to
	 * relation partitioning scheme and no tuples shuffling needed.
	 */
	Distribution dist;
} ExchangePath;

extern Bitmapset *accumulate_part_servers(RelOptInfo *rel);
extern void set_exchange_altrel(ExchangeMode mode, ExchangePath *path,
		RelOptInfo *outerrel, RelOptInfo *innerrel, List *restrictlist,
		Bitmapset *servers);
extern void EXCHANGE_Init_methods(void);
extern List *distributedscan_pathlist(PlannerInfo *root, RelOptInfo *rel,
									  RangeTblEntry *rte);
extern CustomScan *make_exchange(List *custom_plans, List *tlist);
extern ExchangePath *create_exchange_path(PlannerInfo *root, RelOptInfo *rel,
						Path *children, ExchangeMode mode, Distribution dist);
extern void createNodeName(char *nodeName, const char *hostname, int port);
extern void cost_exchange(PlannerInfo *root, RelOptInfo *baserel,
														ExchangePath *expath);

#endif /* EXCHANGE_H_ */
