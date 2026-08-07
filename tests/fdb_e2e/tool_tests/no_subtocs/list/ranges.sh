#!/usr/bin/env bash
set -euxo pipefail
# Ensure that we can wipe specified databases with ranges in the requests
# --> Ensures the MARS requests are working correctly

grib_ls -m data.xxxx.d1.grib

fdb-write data.xxxx.d1.grib
fdb-write data.xxxx.d2.grib
fdb-write data.xxxx.d3.grib
fdb-list --all --minimum-keys="" --porcelain --full | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && echo "Incorrect number of entries written" && exit -1

fdb-list class=rd,expver=xxxx --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && exit -1

fdb-list class=rd,expver=xxxx,date=-1 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && exit -1

fdb-list class=rd,expver=xxxx,date=-1/20170101 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && exit -1

fdb-list class=rd,expver=xxxx,date=-1/20170101/20180103 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && exit -1

fdb-list class=rd,expver=xxxx,date=-1/20170101/20180103/20060101,stream=oper,type=an,levtype=pl,param=60 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && exit -1

fdb-list class=rd,expver=xxxx,date=-1/20170101/20180103,stream=oper,type=an,levtype=pl,param=155 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "36" ]] && exit -1

fdb-list class=rd,expver=xxxx,date=-1/20170101/20180103,stream=oper,type=an,levtype=pl,param=60/155 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "36" ]] && exit -1

fdb-list class=rd,expver=xxxx,date=-1/20170101/20180103,stream=oper,type=an,levtype=pl,param=60/155/138 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && exit -1

fdb-list class=rd,expver=xxxx,date=-1/20170101/20180103,stream=oper,type=an,levtype=pl,param=60/155/138,levelist=300/400/500/700/850/1000 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && exit -1

fdb-list class=rd,expver=xxxx,date=-1/20170101/20180103,stream=oper,type=an,levtype=pl,param=60/155/138,levelist=300/123/1000 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && exit -1

exit 0
