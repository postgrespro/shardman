/*
 * planpass.h
 *
 */

#ifndef PLANPASS_H_
#define PLANPASS_H_

#include "nodes/params.h"
#include "nodes/plannodes.h"

void exec_plan(char *squery, PlannedStmt *pstmt, ParamListInfo paramLI,
														const char *serverName);
void deserialize_plan(char **squery, char **splan, char **sparams);

#endif /* PLANPASS_H_ */
