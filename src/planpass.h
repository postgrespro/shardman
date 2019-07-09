/*
 * planpass.h
 *
 */

#ifndef PLANPASS_H_
#define PLANPASS_H_

#include "nodes/params.h"
#include "nodes/plannodes.h"

void exec_plan(char *squery, PlannedStmt *pstmt, const char *serverName);
void deserialize_plan(char **squery, char **splan);

#endif /* PLANPASS_H_ */
