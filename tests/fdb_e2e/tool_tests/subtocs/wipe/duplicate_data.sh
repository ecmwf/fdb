#!/usr/bin/env bash
set -euxo pipefail

# Ensure that we can wipe specified databases with ranges in the requests
# --> Ensures the MARS requests are working correctly

grib_ls -m data.xxxx.grib

fdb-write data.xxxx.grib
fdb-write data.xxxx.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1

fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1,time=1200
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "FDB modified without --doit" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "FDB modified without --doit" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "FDB modified without --doit" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "FDB modified without --doit" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "8" ]] && echo "FDB modified without --doit" && exit -1


fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1,time=1200 --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Incorrect data files removed" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Incorrect indexes removed" && exit -1

fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1,time=0000 --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect data files removed" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect indexes removed" && exit -1

fdb-write data.xxxx.grib
fdb-write data.xxxx.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1

fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1,time=0000/1200 --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect data files removed" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect indexes removed" && exit -1

exit 0
