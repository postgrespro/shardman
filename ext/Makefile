# the extension name
EXTENSION = hodgepodge
EXTVERSION = 0.0.1
# This file will be executed by CREATE EXTENSION, so let pgxs install it.
DATA = $(EXTENSION)--$(EXTVERSION).sql

REGRESS = shardman_installation

MODULE_big = hodgepodge
OBJS = hodgepodge.o postgres_fdw/postgres_fdw.o postgres_fdw/option.o postgres_fdw/deparse.o postgres_fdw/connection.o postgres_fdw/shippable.o $(WIN32RES)
PGFILEDESC = "A bunch of stuff forming sharding"

ifndef USE_PGXS # hmm, user didn't requested to use pgxs
# relative path to this makefile
mkfile_path := $(word $(words $(MAKEFILE_LIST)),$(MAKEFILE_LIST))
# relative path to dir with this makefile
mkfile_dir := $(dir $(mkfile_path))
# abs path to dir with this makefile
mkfile_abspath := $(shell cd $(mkfile_dir) && pwd -P)
# parent dir name of directory with makefile
parent_dir_name := $(shell basename $(shell dirname $(mkfile_abspath)))
ifneq ($(parent_dir_name),contrib) # a-ha, but this hodgepodge is not inside 'contrib' dir
USE_PGXS := 1 # so use it anyway, most probably that's what the user wants
endif
endif
# $(info) is introduced in 3.81, and PG doesn't support makes older than 3.80
ifeq ($(MAKE_VERSION),3.80)
$(warning $$USE_PGXS is [${USE_PGXS}] (we use it automatically if not in contrib dir))
else
$(info $$USE_PGXS is [${USE_PGXS}] (we use it automatically if not in contrib dir))
endif

ifdef USE_PGXS # use pgxs
# You can specify path to pg_config in PG_CONFIG var
ifndef PG_CONFIG
	PG_CONFIG := pg_config
endif
INCLUDEDIR := $(shell $(PG_CONFIG) --includedir)
PG_CPPFLAGS += -I$(INCLUDEDIR) # add server's include directory for libpq-fe.h
SHLIB_LINK += -lpq # add libpq
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

else # assume the extension is in contrib/ dir of pg distribution
# install and postgres_fdw too
EXTRA_INSTALL = contrib/postgres_fdw
subdir = contrib/hodgepodge
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
