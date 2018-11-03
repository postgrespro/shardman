#!/bin/bash
set -e

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/setup.sh"

if [ -n "$pgpath" ]; then
    export PATH="${pgpath}/bin/:$PATH"
fi
function start_nodes()
{
    echo "Starting nodes"
    for ((i=1; i<=${#worker_datadirs[@]}; ++i)); do
	datadir=${worker_datadirs[i]}
	port=${worker_ports[i]}
	echo "starting $datadir on $port"
	pg_ctl -o "-p $port" -l "/tmp/postgresql_$port.log" -D $datadir start
    done
}

function stop_nodes()
{
    echo "Stopping nodes"
    for datadir in "${worker_datadirs[@]}"; do
	pg_ctl -D $datadir stop || true
    done
}

function restart_nodes()
{
    echo "Restarting nodes"
    for ((i=1; i<=${#worker_datadirs[@]}; ++i)); do
	datadir="${worker_datadirs[i]}"
	port="${worker_ports[i]}"
	pg_ctl -o "-p $port" -D $datadir -l "/tmp/postgresql_$port.log" restart
    done
    pg_ctl -o "-p $lord_port" -D $lord_datadir -l "/tmp/postgresql_${lord_port}.log" restart
}

function send_configs()
{
    echo "Sending configs"
    cat postgresql.conf.common >> ${lord_datadir}/postgresql.conf
    cat postgresql.conf.lord >> ${lord_datadir}/postgresql.conf
    for worker_datadir in "${worker_datadirs[@]}"; do
	cat postgresql.conf.common >> ${worker_datadir}/postgresql.conf
	cat postgresql.conf.worker >> ${worker_datadir}/postgresql.conf
    done

    # custom conf
    if [ -f bin/postgresql.conf.common ]; then
	cat bin/postgresql.conf.common >> ${lord_datadir}/postgresql.conf
	for worker_datadir in "${worker_datadirs[@]}"; do
	    cat bin/postgresql.conf.common >> ${worker_datadir}/postgresql.conf
	done
    fi

    if [ -f pg_hba.conf ]; then
	for datadir in $lord_datadir "${worker_datadirs[@]}"; do
	    cat pg_hba.conf > ${datadir}/pg_hba.conf
	done
    fi
}
