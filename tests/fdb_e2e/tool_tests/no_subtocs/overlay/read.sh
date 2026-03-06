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

# Create a comparison GRIB

grib_copy -w time=0000 data.xxxx.grib data.xxxx.0000.grib
grib_set -s expver=xxxy data.xxxx.0000.grib data.xxxy.0000.grib
grib_set -s expver=xxxy data.xxxx.grib data.xxxy.grib

# Create an overlay to xxxy

fdb-overlay class=rd,expver=xxxx,stream=oper,date=$YESTERDAY,domain=g,time=0000 \
            class=rd,expver=xxxy,stream=oper,date=$YESTERDAY,domain=g,time=0000 \

fdb-read --extract data.xxxy.0000.grib out.grib
grib_compare out.grib data.xxxy.0000.grib

# Show that this is only the data that has been overlaid

fdb-read --extract data.xxxy.grib out2.grib
set +e
grib_compare out2.grib data.xxxy.grib
rc=$?
set -e
[[ $rc = 0 ]] && echo "Retrieved data should be incomplete" && exit -1

exit 0
