#!/bin/bash

EXAMPLE_NAME="3levels-2nodes-36cpn"

export FLUX_SCHED_RC_NOOP="noop"
export FLUX_MODULE_PATH=${FLUX_MODULE_PATH:+"$FLUX_MODULE_PATH:"}"../../src"

LOG_DIR=./log
RESULTS_DIR=./results

mkdir -p ${RESULTS_DIR}
mkdir -p ${LOG_DIR}

NUM_JOBS=3000
COMMAND="sleep 0"

srun -N 2 flux start \
     python ../../src/initial_program \
     ./hierarchy-config.json \
     --log_dir ./log --results_dir ./results \
     root ${NUM_JOBS} ${COMMAND}
