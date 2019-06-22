# the extension name
EXTENSION = shardman
EXTVERSION = 0.0.1
# This file will be executed by CREATE EXTENSION, so let pgxs install it.
DATA = $(EXTENSION)--$(EXTVERSION).sql

REGRESS = shardman_installation

MODULE_big = shardman
OBJS =	src/common.o src/dmq.o src/exchange.o src/expath.o src/hooks.o \
		src/meta.o src/nodeDistPlanExec.o src/nodeDummyscan.o src/partutils.o \
		src/planpass.o src/sbuf.o src/shardman.o src/stream.o $(WIN32RES)
PGFILEDESC = "Shardman extension"

# we assume the extension is in contrib/ dir of pg distribution
fdw_srcdir = $(top_srcdir)/contrib/postgres_fdw/
top_builddir = ../..
fdw_builddir = $(top_builddir)/contrib/postgres_fdw/

PG_CPPFLAGS = -I$(libpq_srcdir) -I$(fdw_srcdir)
# XXX sometimes PKGLIBDIR where postgres_Fdw.so lies is not the same as LIBDIR:
# postgresql/ is appended to the former (c.f. Makefile.global.in:120).
# So add it to rpath; probably there is a better way?
# XXX I would add rpath to PG_LDFLAGS, but it turned out that due to expansion
# order PG_LDFLAGS existence leaves SHLIB_LINK_INTERNAL ignored.
SHLIB_LINK_INTERNAL = -Wl,-rpath,'$(rpathdir)/postgresql' $(libpq) -L$(fdw_builddir) -l:postgres_fdw.so

SHLIB_PREREQS = submake-libpq # make libpq first
# XXX make postgres_fdw first?
subdir = contrib/shardman
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
