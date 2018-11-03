# pgpath=~/postgres/install/shardman_musl
start_port=5432

etcd_datadir=/tmp/etcd_datadir
datadir_prefix="/tmp"
# datadir_prefix="${HOME}/tmp/tmp"
clusters=3
instances=2

export STOLONCTL_STORE_BACKEND=etcdv3
export STSENTINEL_STORE_BACKEND=etcdv3
export STKEEPER_STORE_BACKEND=etcdv3

declare -a datadirs=()
declare -a ports=()
i=0
for cluster in $(seq 1 $clusters); do
    for inst in $(seq 1 $instances); do
	datadirs[$i]="${datadir_prefix}/data_${cluster}_${inst}"
	ports[$i]=$(($start_port + $i))
	let "i+=1"
    done
done
echo ${datadirs[@]}
echo ${ports[@]}

function run_demo()
{
    :
}
