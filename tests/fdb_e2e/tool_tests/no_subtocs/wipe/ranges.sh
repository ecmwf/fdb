#!/usr/bin/env bash
set -euxo pipefail

# Ensure that we can wipe specified databases with ranges in the requests
# --> Ensures the MARS requests are working correctly

grib_ls -m data.xxxx.d1.grib

fdb-write data.xxxx.d1.grib
fdb-write data.xxxx.d2.grib
fdb-write data.xxxx.d3.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && echo "Incorrect number of entries written" && exit -1

fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1/20180103,time=1200
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && echo "FDB modified without --doit" && exit -1

fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1/20180103,time=1200 --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "36" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep date=20170101 out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep date=20180103 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Incorrect data files removed" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Incorrect indexes removed" && exit -1

fdb-wipe class=rd,expver=xxxx,stream=oper,date=20170101,time=0000/1200 --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep date=20170101 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep date=20180103 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Incorrect data files removed" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Incorrect indexes removed" && exit -1

fdb-write data.xxxx.d1.grib
fdb-write data.xxxx.d2.grib
fdb-write data.xxxx.d3.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "96" ]] && echo "Incorrect number of entries written" && exit -1

fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1/20170101/20180103,time=0000/1200
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "96" ]] && echo "FDB modified without --doit" && exit -1

fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1/20170101/20180103,time=0000/1200 --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && echo "All data should have been deleted" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect data files removed" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect indexes removed" && exit -1

exit 0
