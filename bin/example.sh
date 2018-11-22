#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

hpctl init --cluster-name haha --store-endpoints localhost:2379 --pg-su-auth-method trust
hpctl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_1
hpctl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_2
hpctl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_3
hpctl hash-shard-table --cluster-name haha --relname pt --sql "create table pt(id serial, payload real) partition by hash(id);" --numparts 10
hpctl hash-shard-table --cluster-name haha --relname pt2 --sql "create table pt2(id serial, payload real) partition by hash(id);" --numparts 10 --colocate-with pt
psql -c "insert into pt select a.i, a.i from (select generate_series(1, 1000) i) a;"
# don't forget to disable creating indexes of fdw tables
# hpctl hash-shard-table --cluster-name haha --relname pt --sql "create table pti(id serial primary key, payload real) partition by hash(id);" --numparts 10

hpctl rebalance --cluster-name haha -p 10

hpctl drop-table --cluster-name haha --relname pt

hpctl rmrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_1
hpctl rmrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_2
hpctl rmrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_3

# enable global snapshots
hpctl --cluster-name haha update --patch -f "${script_dir}"/global_snapshots.json

hpctl --cluster-name haha update --patch '{ "pgParameters": {"shared_preload_libraries" : "hodgepodge" }}'
hpctl forall --cluster-name haha  --sql "drop extension hodgepodge cascade;"
hpctl forall --cluster-name haha  --sql "create extension hodgepodge;"

hpmon --cluster-name haha

make -C ~/postgres/hodgepodge/ext/ clean && make -C ~/postgres/hodgepodge/ext/ install && hpctl forall --cluster-name haha  --sql "drop extension hodgepodge;" && hpctl forall --cluster-name haha  --sql "create extension hodgepodge;"
