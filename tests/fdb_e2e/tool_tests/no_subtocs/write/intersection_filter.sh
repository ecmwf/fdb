#!/usr/bin/env bash
set -euxo pipefail

# test the INCLUDE FILTER

grib_ls -m data.xxxx.grib

fdb-write --exclude-filter=time=1200 --include-filter=time=1200 data.xxxx.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && echo "Data written when all should be excluded" && exit -1

fdb-write --exclude-filter=time=1200/0000,levelist=500/to/1500/by/100 --include-filter=time=1200 data.xxxx.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "6" ]] && echo "Incorrect number of elements written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "6" ]] && echo "Incorrect amount of data written" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data written" && exit -1

exit 0
