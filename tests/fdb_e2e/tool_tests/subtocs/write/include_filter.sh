#!/usr/bin/env bash
set -euxo pipefail

# test the INCLUDE FILTER

grib_ls -m data.xxxx.grib

fdb-write --include-filter=time=0000 data.xxxx.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "12" ]] && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "12" ]] && exit -1
[[ "$(grep -v time=0000 out | wc -l | tr -d '[:space:]')" != "0" ]] && exit -1

# If we filter on a non-existent experiment, we should get no data
fdb-wipe --doit --minimum-keys=class,expver class=rd,expver=xxxx
fdb-write --include-filter=expver=yyyy data.xxxx.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && exit -1

fdb-write --include-filter=param=vo data.xxxx.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "12" ]] && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "6" ]] && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "6" ]] && exit -1

exit 0
