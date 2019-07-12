/*-------------------------------------------------------------------------
 *
 * common.h
 *	Common code for ParGRES extension
 *
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * Author: Andrey Lepikhov <a.lepikhov@postgrespro.ru>
 *
 * IDENTIFICATION
 *	contrib/pargres/common.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMMON_H_
#define COMMON_H_

#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "storage/lock.h"
#include "dmq.h"

typedef char NodeName[256];

typedef struct
{
	Oid					serverid;
	NodeName			node;
	DmqDestinationId	dest_id;
} DMQDestinations;

typedef struct
{
	int				nservers;
	int				coordinator_num;
	DMQDestinations	*dests;
} DMQDestCont;

typedef struct
{
	LWLock	*lock;

	/* It stores existed DMQ connections. Shared between backends. */
	HTAB	*htab;

	/* Used for unique stream name generation in EXCHANGE node */
	volatile pg_atomic_uint64	exchangeID;
} ExchangeSharedState;


extern MemoryContext		DPGMemoryContext;
extern ExchangeSharedState	*DPGShmem;
extern bool					enable_distributed_execution;


extern bool plan_tree_walker(Plan *plan, bool (*walker) (), void *context);
extern bool path_walker(const Path *path, bool (*walker) (), void *context);
extern void OnNodeDisconnect(const char *node_name);

#endif /* COMMON_H_ */
