#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

shardmanctl init --cluster-name haha --store-endpoints localhost:2379  -f hpspec.json
shardmanctl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_1
shardmanctl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_2
shardmanctl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_3

psql -p 5433 -c "create table pt (id serial, payload real) partition by hash(id);"
psql -p 5433 -c "select shardman.hash_shard_table('pt', 10)"
psql -p 5433 -c "insert into pt select a.i, a.i from (select generate_series(1, 1000) i) a;"
# don't forget to disable creating indexes of fdw tables
# shardmanctl hash-shard-table --cluster-name haha --relname pt --sql "create table pti(id serial primary key, payload real) partition by hash(id);" --numparts 10

shardmanctl rebalance --cluster-name haha -p 10

shardmanctl rmrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_1
shardmanctl rmrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_2
shardmanctl rmrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_3

# enable global snapshots
shardmanctl --cluster-name haha update --patch -f "${script_dir}"/global_snapshots.json

shardmanctl --cluster-name haha update --patch '{ "pgParameters": {"shared_preload_libraries" : "shardman" }}'
shardmanctl forall --cluster-name haha  --sql "drop extension shardman cascade;"
shardmanctl forall --cluster-name haha  --sql "create extension shardman;"

shardman-monitor --cluster-name haha

make -C ~/postgres/shardman/ext/ clean && make -C ~/postgres/shardman/ext/ install && shardmanctl forall --cluster-name haha  --sql "drop extension shardman;" && shardmanctl forall --cluster-name haha  --sql "create extension shardman;"

###################################

shardman-ladle --cluster-name haha init -f shmnspec.json
shardman-ladle --cluster-name haha addnodes -n vg1,vg2,vg3
