#!/usr/bin/env bash
set -euxo pipefail

grib_ls -m data.xxxy.grib

fdb-write data.xxxx.grib
fdb-write data.xxxy.grib
fdb-write data.xxxz.grib

fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "3" ]] && echo "Incorrect number of entries written" && exit -1

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "3" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxz" out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1

# Create a request for reading (only selects
fdb-read req out.grib
grib_compare out.grib data.xxxy.grib

exit 0
