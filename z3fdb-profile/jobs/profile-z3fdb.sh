#!/bin/bash
# The job name
#SBATCH --job-name=profile-z3fdb
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
set -euo pipefail

my_jobs_root="$HOME/z3fdb-profile"
source "$my_jobs_root/jobs/utils.sh"

workspace=${HOME}/workspace
tool=${workspace}/fdb/tools/z3fdb-profiler/z3fdb-profiler

source ${workspace}/environment-local-build
source ${HOME}/python-venvs/bench/bin/activate

views=(
    "era5_test.yaml"
)

for view in "${views[@]}"; do
	echo "Profiling view ${view%.yaml}"
	rm -rf ${view%.yaml}
	mkdir -p ${view%.yaml}
	cd ${view%.yaml}
	perf record \
	    -F 500 \
	    --call-graph dwarf,16384 \
	    python ${tool} -p \
	    aggregate \
	    --limit 10000 \
		--view ${workspace}/fdb/tools/z3fdb-profiler/data/${view} \
		--fdb-config "$SCRATCH/fdb-store/fdb_config.yaml"
	perf script > out.perf
	${workspace}/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
	${workspace}/FlameGraph/flamegraph.pl out.folded > ${view%.yaml}.svg
	${workspace}/FlameGraph/flamegraph.pl --reverse out.folded > ${view%.yaml}_rev.svg
	cd ..
done

upload_files $(date +%Y-%m-%dT%H:%M:%S) **/*.svg
