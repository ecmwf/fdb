#!/usr/bin/env bash
set -euxo pipefail

grib_ls -m data.steprange.xxxy.grib

fdb-write data.steprange.xxxx.grib
fdb-write data.steprange.xxxy.grib
fdb-write data.steprange.xxxz.grib

fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "3" ]] && echo "Incorrect number of entries written" && exit -1

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "3" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxz" out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1

# Create a request for reading (only selects

fdb-read req.xxxy.steprange out.grib
grib_compare out.grib data.steprange.xxxy.grib

exit 0
