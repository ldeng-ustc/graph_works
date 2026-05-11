#!/bin/bash

SCRIPT_PATH=$(dirname "$(readlink -f "$0")")

mkdir -p ${SCRIPT_PATH}/build
cd ${SCRIPT_PATH}/build
cmake ..
make -j8 bench_run_algo

bin_path_prefix=${SCRIPT_PATH}/build/bin
db_path=/data/lsmgraph_data

suffix=.e.b.random.b # or .e

dataset_path=/dataset
dataset_name=google
dataset_file=${dataset_path}/${dataset_name}/${dataset_name}${suffix}
echo ${dataset_file}

mkdir -p "${db_path}/${dataset_name}"

exe_app=bench_run_algo

echo -e "\nStarting at $(date)" | tee -a $schedule

alg_app=scan,sssp,bfs,cc
com_limit_mem=16
read_p=10000000000

cmd="${bin_path_prefix}/${exe_app} \
                 -dataset_name=${dataset_name} \
                 -alg_app=${alg_app} \
                 -max_subcompactions=8 \
                 -thread_num=16 \
                 -shuffle_edge=true \
                 -dataset_path=${dataset_file} \
                 -memtable_num=2 \
                 -db_path=${db_path}/${dataset_name} \
                 -support_mulversion=1 \
                 -test_times=1 \
                 -read_p=${read_p} \
                 -del_rate=0 \
                 -reserve_node=1 \
                 -khop=1 \
                 -khop_num=100 \
                 -source=20"

echo cmd: ${cmd}
eval "${cmd}"