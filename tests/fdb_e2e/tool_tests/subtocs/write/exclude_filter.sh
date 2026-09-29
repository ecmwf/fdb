#!/usr/bin/env bash
set -euxo pipefail

# test the EXCLUDE FILTER

grib_ls -m data.xxxx.grib

fdb-write --exclude-filter=levelist=800/to/900/by/50 data.xxxx.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "20" ]] && exit -1
[[ "$(grep levelist=850 out | wc -l | tr -d '[:space:]')" != "0" ]] && exit -1

fdb-wipe --doit --minimum-keys=class,expver class=rd,expver=xxxx
fdb-write --exclude-filter=time=0000 data.xxxx.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "12" ]] && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "0" ]] && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "12" ]] && exit -1

fdb-wipe --doit --minimum-keys=class,expver class=rd,expver=xxxx
fdb-write --exclude-filter=time=0000/1200,levelist=500/to/1500/by/100 data.xxxx.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "12" ]] && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "6" ]] && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "6" ]] && exit -1

exit 0

