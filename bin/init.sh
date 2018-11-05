#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

# install ext
cd "${script_dir}/../ext"
make clean
make install

# build go
cd "${script_dir}/../go"
make

pkill stolon-keeper || true
pkill stolon-sentinel || true
pkill -9 postgres || true
pkill etcd || true

rm -rf "{etcd_datadir}"
nohup etcd --data-dir "${etcd_datadir}" >/tmp/etcd.log 2>&1 &
# let etcd start
sleep 5

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

run_demo

# psql
