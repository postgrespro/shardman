#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

hpctl init --cluster-name haha --store-endpoints localhost:2379 --pg-su-auth-method trust
hpctl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_1
hpctl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_2
hpctl hash-shard-table --cluster-name haha --relname pt --sql "create table pt(id serial, payload real) partition by hash(id);" --numparts 10
# don't forget to disable creating indexes of fdw tables
# hpctl hash-shard-table --cluster-name haha --relname pt --sql "create table pti(id serial primary key, payload real) partition by hash(id);" --numparts 10

hpctl rmrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_1
hpctl rmrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_2
