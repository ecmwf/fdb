#!/usr/bin/env bash
set -euxo pipefail

YESTERDAY=$1

grib_ls -m data.xxxx.grib

fdb-write data.xxxx.grib

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "time=1200" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1

# Create an overlay to xxxy

fdb-overlay class=rd,expver=xxxx,stream=oper,date=$YESTERDAY,domain=g,time=0000 \
            class=rd,expver=xxxy,stream=oper,date=$YESTERDAY,domain=g,time=0000

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "36" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "time=1200" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=1200" | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1

# Wipe the DB. Should fail (no wipeable data found)

set +e
fdb-wipe class=rd,expver=xxxy,stream=oper,date=$YESTERDAY,domain=g,time=0000 --doit
rc=$?
set -e

[[ $rc = 0 ]] && echo "Should be nothing to wipe with overlayed data" && exit 1

# Write directly to the target

fdb-write data.xxxy.grib

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "60" ]] && echo "Incorrect number of entries written" && exit -1

# We can wipe the data that we added, but don't impact the written data

fdb-wipe class=rd,expver=xxxy,stream=oper,date=$YESTERDAY,domain=g,time=1200/0000 --doit

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "36" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "time=1200" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=1200" | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1

exit 0
