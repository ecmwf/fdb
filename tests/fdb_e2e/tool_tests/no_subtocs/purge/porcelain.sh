#!/usr/bin/env bash
set -euxo pipefail

grib_ls -m data.xxxx.grib

fdb-write data.xxxx.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1

fdb-write data.xxxx.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Data not correctly duplicated" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly duplicated" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly duplicated" && exit -1

fdb-purge class=rd,expver=xxxx,stream=oper,date=-1,time=1200 | tee purge_out
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "FDB modified without --doit" && exit -1
(( "$(wc -l < purge_out | tr -d '[:space:]')" < "10" )) && echo "normal purge output incomplete" && exit -1

fdb-purge class=rd,expver=xxxx,stream=oper,date=-1,time=1200 --porcelain | tee purge_out
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "FDB modified without --doit" && exit -1
[[ "$(wc -l < purge_out | tr -d '[:space:]')" != "2" ]] && echo "Unexpected file list for removal" && exit -1

fdb-purge class=rd,expver=xxxx,stream=oper,date=-1,time=1200 --porcelain --doit | tee purge_out
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "36" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(wc -l < purge_out | tr -d '[:space:]')" != "2" ]] && echo "Unexpected file list for removal" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "3" ]] && echo "Incorrect data files removed" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "3" ]] && echo "Incorrect indexes removed" && exit -1

exit 0
