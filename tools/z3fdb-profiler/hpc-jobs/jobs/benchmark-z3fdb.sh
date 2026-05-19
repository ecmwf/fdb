#!/bin/bash
# The job name
#SBATCH --job-name=benchmar-z3fdb
# Set the error and output files
#SBATCH --output=%Jout.txt
#SBATCH --error=%Jerr.txt
# Choose the queue
#SBATCH --qos=np
# Wall clock time limit
#SBATCH --time=00:20:00
# Send an email on failure
#SBATCH --mail-type=ALL
# This is the job
#SBATCH --array=0-99
set -euo pipefail

my_jobs_root="$HOME/z3fdb-profile"
source "$my_jobs_root/jobs/utils.sh"

workspace=${HOME}/workspace
tool=${workspace}/fdb/tools/z3fdb-profiler/z3fdb-profiler

job_id="$SLURM_ARRAY_JOB_ID"
task_id="$SLURM_ARRAY_TASK_ID"
log_file="log_$task_id.txt"

source ${workspace}/environment-local-build
source ${HOME}/python-venvs/bench/bin/activate

view="era5.yaml"

echo "Running random access on z3fdb view ${view%.yaml}"
python ${tool} -p \
    aggregate \
    --limit 10000 \
    --view ${workspace}/fdb/tools/z3fdb-profiler/data/${view} \
    --fdb-config "$SCRATCH/fdb-store/fdb_config.yaml" | tee $log_file

upload_files  ${SLURM_JOB_NAME}_${job_id} $log_file
