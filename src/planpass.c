/*
 * serialize.c
 *
 */

#include "postgres.h"

#include "common/base64.h"
#include "executor/executor.h"
#include "tcop/tcopprot.h"
#include "utils/plancache.h"
#include "utils/snapmgr.h"

#include "nodeDistPlanExec.h"
#include "planpass.h"

void
exec_plan(char *squery, PlannedStmt *pstmt, const char *serverName)
{
	CachedPlanSource	*psrc;
	CachedPlan			*cplan;
	QueryDesc			*queryDesc;
	DestReceiver		*receiver;
	int					eflags = 0;
	Oid					*param_types = NULL;

	Assert(squery && pstmt);
	debug_query_string = strdup(squery);
	psrc = CreateCachedPlan(NULL, squery, NULL);

	CompleteCachedPlan(psrc, NIL, NULL, param_types, 0, NULL,
								NULL, CURSOR_OPT_GENERIC_PLAN, false);

	SetRemoteSubplan(psrc, pstmt);
	cplan = GetCachedPlan(psrc, NULL, false, NULL);

	receiver = CreateDestReceiver(DestNone);

	PG_TRY();
	{
		lcontext context;
		queryDesc = CreateQueryDesc(pstmt,
									squery,
									GetActiveSnapshot(),
									InvalidSnapshot,
									receiver,
									NULL, NULL,
									0);

		context.pstmt = pstmt;
		context.eflags = eflags;
		context.servers = NULL;
		context.indexinfo = NULL;
		localize_plan(pstmt->planTree, &context);

		ExecutorStart(queryDesc, eflags);
		EstablishDMQConnections(&context, serverName, queryDesc->estate,
								queryDesc->planstate);
		PushActiveSnapshot(queryDesc->snapshot);
		ExecutorRun(queryDesc, ForwardScanDirection, 0, true);
		PopActiveSnapshot();
		ExecutorFinish(queryDesc);
		ExecutorEnd(queryDesc);
		FreeQueryDesc(queryDesc);
	}
	PG_CATCH();
	{
		elog(INFO, "BAD QUERY: '%s'.", squery);
		ReleaseCachedPlan(cplan, false);
		PG_RE_THROW();
	}
	PG_END_TRY();

	receiver->rDestroy(receiver);
	ReleaseCachedPlan(cplan, false);
}

/*
 * Decode base64 string into C-string and return it at same pointer
 */
void
deserialize_plan(char **squery, char **splan)
{
	char	*dec_query,
			*dec_plan;
	int		dec_query_len,
			len,
			dec_plan_len;

	dec_query_len = pg_b64_dec_len(strlen(*squery));
	dec_query = palloc0(dec_query_len + 1);
	len = pg_b64_decode(*squery, strlen(*squery), dec_query);
	Assert(dec_query_len >= len);

	dec_plan_len = pg_b64_dec_len(strlen(*splan));
	dec_plan = palloc0(dec_plan_len + 1);
	len = pg_b64_decode(*splan, strlen(*splan), dec_plan);
	Assert(dec_plan_len >= len);

	*squery = dec_query;
	*splan = dec_plan;
}

