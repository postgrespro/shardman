/* -------------------------------------------------------------------------
 *
 * hodgepodge.c
 *
 * Copyright (c) 2018, Postgres Professional
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/guc.h"

/* ensure that extension won't load against incompatible version of Postgres */
PG_MODULE_MAGIC;

/* GUC variables */
static int node_id;

extern void _PG_init(void);

/*
 * Entrypoint of the module. Define GUCs.
 */
void
_PG_init()
{
	DefineCustomIntVariable(
		"hodgepodge.node_id",
		"Node id",
		NULL,
		&node_id,
		-1, -1, 4096, /* boot, min, max */
		PGC_SUSET,
		0,
		NULL, NULL, NULL);
}
