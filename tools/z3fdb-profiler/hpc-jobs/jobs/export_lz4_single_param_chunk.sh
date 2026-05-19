#!/bin/bash
# The job name
#SBATCH --job-name=export-as-zarr-lz4
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
dest=${SCRATCH}/zarr-store-era5-lz4-5

source ${workspace}/environment-local-build
source ${HOME}/python-venvs/bench/bin/activate

rm -rf ${dest}

${tool} -p export \
	--view ${workspace}/fdb/tools/z3fdb-profiler/data/era5.yaml \
    --fdb-config "$SCRATCH/fdb-store/fdb_config.yaml" \
    --blosc-cname="lz4" \
    --blosc-clevel=5 \
	-o ${dest}

