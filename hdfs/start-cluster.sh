#!/usr/bin/env bash

script_dir="$(cd "$(dirname "$0")" && pwd)"
log_dir="${script_dir}/logs"

source "${script_dir}/nodes.sh"

echo "Installing..."
go install controller/controller.go   || exit 1 # Exit if compile+install fails
go install nodes/storagenode.go || exit 1 # Exit if compile+install fails
echo "Done!"

echo "Creating log directory: ${log_dir}"
mkdir -pv "${log_dir}"

echo "Starting Controller..."
ssh "${controller}" "${HOME}/go/bin/controller $1 $2/Controller" &> "${log_dir}/controller.log" &

echo "Starting Storage Nodes..."
length=${#nodes[@]}
for (( i = 0; i < length; i++ )); do
	echo "${nodes[$i]}"
	ssh "${nodes[$i]}" "${HOME}/go/bin/storagenode $controller $1 $(($1 + $i + 1)) $2/StorageNode$(($i + 1))" &> "${log_dir}/${nodes[$i]}.log" &
done

echo "Startup complete!"