#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

hpctl init --cluster-name haha --store-endpoints localhost:2379 --pg-su-auth-method trust
hpctl addrepgroup --cluster-name haha --store-endpoints localhost:2379 --stolon-name cluster_1
