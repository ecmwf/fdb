#!/usr/bin/env bash
set -euxo pipefail

grib_ls -m data.source.xxxy.grib

fdb-write data.source.xxxx.grib
fdb-write data.source.xxxy.grib
fdb-write data.source.xxxz.grib

fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && echo "Incorrect number of entries written" && exit -1
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxz" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=1200" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1


# Create a subselected GRIB for comparison purposes

grib_copy -w time=1200 data.source.xxxy.grib data.12.xxxy.grib

# Create a request for reading (only selects

fdb-read req.xxxy.simple out.grib
grib_compare out.grib data.12.xxxy.grib

exit 0
