#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

cd "${script_dir}/.."
go install

pkill stolon-keeper || true
pkill stolon-sentinel || true
pkill -9 postgres || true
pkill etcd || true

rm -rf "{etcd_datadir}"
nohup etcd --data-dir "${etcd_datadir}" >/tmp/etcd.log 2>&1 &

i=0
for cluster in $(seq 1 $clusters); do
    echo "Init cluster cluster_${cluster}"
    stolonctl init --yes --cluster-name "cluster_${cluster}"
    echo "Starting sentinel for cluster_${cluster}"
    nohup stolon-sentinel --cluster-name "cluster_${cluster}" >/tmp/sentinel_$cluster.log 2>&1 &
    for inst in $(seq 1 $instances); do
	echo "Starting keeper keeper_${inst} at ${datadirs[i]}"
	nohup stolon-keeper --cluster-name "cluster_${cluster}" --data-dir "${datadirs[i]}" --pg-listen-address "localhost" --pg-port "${ports[i]}" --uid "keeper_${inst}" --pg-repl-username repluser --pg-repl-auth-method trust --pg-su-auth-method trust >/tmp/keeper_${cluster}_${inst}.log 2>&1 &
	let "i+=1"
    done
done

exit 1
for datadir in $lord_datadir "${worker_datadirs[@]}"; do
    rm -rf "$datadir"
    mkdir -p "$datadir"
    initdb --no-sync -D "$datadir"
done

send_configs

start_nodes
for port in $lord_port "${worker_ports[@]}"; do
    createdb -p $port `whoami`
    echo "creating pg_shardman extension..."
    psql -p $port -c "create extension pg_shardman cascade;"
done

restart_nodes

run_demo

# psql
