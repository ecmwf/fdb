#!/bin/bash
# The job name
#SBATCH --job-name=compare-zarr-fdb
# Set the error and output files
#SBATCH --output=%Jout.txt
#SBATCH --error=%Jerr.txt
# Choose the queue
#SBATCH --qos=np
# Wall clock time limit
#SBATCH --time=48:00:00
# Send an email on failure
#SBATCH --mail-type=ALL
# This is the job
set -ex

workspace=${HOME}/workspace
tool=${workspace}/fdb/tools/z3fdb-profiler/z3fdb-profiler
store="zarr-store-era5"

source ${workspace}/environment-local-build
source ${HOME}/python-venvs/bench/bin/activate

rm -rf ${dest}

${tool} -p compare \
	--view ${workspace}/fdb/tools/z3fdb-profiler/data/era5.yaml \
    --fdb-config "$SCRATCH/fdb-store/fdb_config.yaml" \
    --zarr=$SCRATCH/$store 

