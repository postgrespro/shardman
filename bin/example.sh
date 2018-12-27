#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

hodgepodgectl init --cluster-name haha --store-endpoints localhost:2379  -f hpspec.json
hodgepodgectl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_1
hodgepodgectl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_2
hodgepodgectl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_3

psql -p 5433 -c "create table pt (id serial, payload real) partition by hash(id);"
psql -p 5433 -c "select hodgepodge.hash_shard_table('pt', 10)"
psql -p 5433 -c "insert into pt select a.i, a.i from (select generate_series(1, 1000) i) a;"
# don't forget to disable creating indexes of fdw tables
# hodgepodgectl hash-shard-table --cluster-name haha --relname pt --sql "create table pti(id serial primary key, payload real) partition by hash(id);" --numparts 10

hodgepodgectl rebalance --cluster-name haha -p 10

hodgepodgectl rmrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_1
hodgepodgectl rmrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_2
hodgepodgectl rmrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_3

# enable global snapshots
hodgepodgectl --cluster-name haha update --patch -f "${script_dir}"/global_snapshots.json

hodgepodgectl --cluster-name haha update --patch '{ "pgParameters": {"shared_preload_libraries" : "hodgepodge" }}'
hodgepodgectl forall --cluster-name haha  --sql "drop extension hodgepodge cascade;"
hodgepodgectl forall --cluster-name haha  --sql "create extension hodgepodge;"

hodgepodge-monitor --cluster-name haha

make -C ~/postgres/hodgepodge/ext/ clean && make -C ~/postgres/hodgepodge/ext/ install && hodgepodgectl forall --cluster-name haha  --sql "drop extension hodgepodge;" && hodgepodgectl forall --cluster-name haha  --sql "create extension hodgepodge;"

###################################

hodgepodge-ladle --cluster-name haha init -f hpspec.json
hodgepodge-ladle --cluster-name haha addnodes -n vg1,vg2,vg3
