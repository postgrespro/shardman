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

#define HP_TAG "[HP %d] "
#define hp_elog(level,fmt,...) elog(level, HP_TAG fmt, MyRgid, ## __VA_ARGS__)

#if DEBUG_LEVEL == 0
#define hp_log1(fmt, ...) elog(LOG, HP_TAG fmt, MyRgid, ## __VA_ARGS__)
#define hp_log2(fmt, ...)
#define hp_log3(fmt, ...)
#elif DEBUG_LEVEL == 1
#define hp_log1(fmt, ...) elog(LOG, HP_TAG fmt, MyRgid, ## __VA_ARGS__)
#define hp_log2(fmt, ...) elog(LOG, HP_TAG fmt, MyRgid, ## __VA_ARGS__)
#define hp_log3(fmt, ...)
#elif DEBUG_LEVEL == 2
#define hp_log1(fmt, ...) elog(LOG, HP_TAG fmt, MyRgid, ## __VA_ARGS__)
#define hp_log2(fmt, ...) elog(LOG, HP_TAG fmt, MyRgid, ## __VA_ARGS__)
#define hp_log3(fmt, ...) elog(LOG, HP_TAG fmt, MyRgid, ## __VA_ARGS__)
#endif

extern int MyRgid;

extern void _PG_init(void);

#endif /* SHARDMAN_H */
