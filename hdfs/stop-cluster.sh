#!/usr/bin/env bash

script_dir="$(cd "$(dirname "$0")" && pwd)"
log_dir="${script_dir}/logs"

source "${script_dir}/nodes.sh"

echo "Stopping controller..."
ssh "${controller}" 'pkill -u '$(whoami)' controller'

echo "Stopping Storage Nodes..."
for node in ${nodes[@]}; do
    echo "${node}"
    ssh "${node}" "rm -rf $1/Controller $1/StorageNode* $1/Client"
    ssh "${node}" "pkill -u "$(whoami)" storagenode"
done

echo "Deleting logs..."
ssh "${controller}" 'rm -rf /home/'$(whoami)'/bigdata-project/hdfs/logs'

echo "Done!"
