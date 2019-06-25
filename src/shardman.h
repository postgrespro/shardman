/* -------------------------------------------------------------------------
 *
 * shardman.h
 *
 * Copyright (c) 2018, Postgres Professional
 *
 * -------------------------------------------------------------------------
 */

#ifndef __SHARDMAN_H__
#define __SHARDMAN_H__

/* #ifndef DEBUG_LEVEL */
/* #define DEBUG_LEVEL 0 */
/* #endif */
#define DEBUG_LEVEL 1

#define SHMN_TAG "[SHMN %d] "
#define shmn_elog(level,fmt,...) elog(level, SHMN_TAG fmt, MyRgid, ## __VA_ARGS__)

#if DEBUG_LEVEL == 0
#define shmn_log1(fmt, ...) elog(LOG, SHMN_TAG fmt, MyRgid, ## __VA_ARGS__)
#define shmn_log2(fmt, ...)
#define shmn_log3(fmt, ...)
#elif DEBUG_LEVEL == 1
#define shmn_log1(fmt, ...) elog(LOG, SHMN_TAG fmt, MyRgid, ## __VA_ARGS__)
#define shmn_log2(fmt, ...) elog(LOG, SHMN_TAG fmt, MyRgid, ## __VA_ARGS__)
#define shmn_log3(fmt, ...)
#elif DEBUG_LEVEL == 2
#define shmn_log1(fmt, ...) elog(LOG, SHMN_TAG fmt, MyRgid, ## __VA_ARGS__)
#define shmn_log2(fmt, ...) elog(LOG, SHMN_TAG fmt, MyRgid, ## __VA_ARGS__)
#define shmn_log3(fmt, ...) elog(LOG, SHMN_TAG fmt, MyRgid, ## __VA_ARGS__)
#endif

extern int MyRgid;

extern void _PG_init(void);

#endif /* SHARDMAN_H */
