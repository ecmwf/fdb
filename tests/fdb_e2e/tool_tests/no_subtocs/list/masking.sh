#!/usr/bin/env bash
set -euxo pipefail

# Ensure that we can wipe specified databases with ranges in the requests
# --> Ensures the MARS requests are working correctly

grib_ls -m data.xxxx.d1.grib

fdb-write data.xxxx.d1.grib
fdb-write data.xxxx.d2.grib
fdb-write data.xxxx.d1.grib
fdb-write data.xxxx.d2.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
fdb-list --all --minimum-keys="" --porcelain --full | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "96" ]] && echo "Incorrect number of entries written" && exit -1

fdb-list class=rd,expver=xxxx,date=-1/20170101,stream=oper,type=an,levtype=pl,param=155/138,levelist=300/400/500/700/850/1000 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Entries should be masked without --full" && exit -1
fdb-list class=rd,expver=xxxx,date=-1/20170101,stream=oper,type=an,levtype=pl,param=155/138,levelist=300/400/500/700/850/1000 --porcelain --full | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "96" ]] && echo "All entries should be visible with --full" && exit -1

fdb-list class=rd,expver=xxxx,date=-1/20170101,stream=oper,type=an,levtype=pl,param=130/138,levelist=300/123/1000 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "8" ]] && echo "Entries should be masked without full" && exit -1
fdb-list class=rd,expver=xxxx,date=-1/20170101,stream=oper,type=an,levtype=pl,param=130/138,levelist=300/123/1000 --porcelain --full | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "16" ]] && echo "All entries should be visible with --full" && exit -1

exit 0
