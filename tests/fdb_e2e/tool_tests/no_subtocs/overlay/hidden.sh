#!/usr/bin/env bash
set -euxo pipefail

YESTERDAY=$1

grib_ls -m data.xxxx.grib

fdb-write data.xxxx.grib

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "step=0" | grep "time=1200" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "step=0" | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1

# Create an existing DB, with existing Data

fdb-write data.xxxy.3.grib

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=1200" | grep "step=3" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=0000" | grep "step=3" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1

# Hide the existing DB

fdb-hide class=rd,expver=xxxy,stream=oper,date=$YESTERDAY,domain=g,time=1200 --doit
fdb-hide class=rd,expver=xxxy,stream=oper,date=$YESTERDAY,domain=g,time=0000 --doit

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Data should be hidden" && exit -1
[[ "$(grep "expver=xxxy" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data should be hidden" && exit -1

# Create an overlay to xxxy -- fails without --doit

set +e
fdb-overlay class=rd,expver=xxxx,stream=oper,date=$YESTERDAY,domain=g,time=0000 \
            class=rd,expver=xxxy,stream=oper,date=$YESTERDAY,domain=g,time=0000
rc=$?
set -e
[[ $rc = 0 ]] && echo "Overlay should not succeed against existing DB" && exit 1

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Data shouldn't change after failing overlay" && exit -1
[[ "$(grep "expver=xxxx" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Data shouldn't change after failing overlay" && exit -1
[[ "$(grep "expver=xxxx" out | grep "step=0" | grep "time=1200" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Data shouldn't change after failing overlay" && exit -1
[[ "$(grep "expver=xxxx" out | grep "step=0" | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Data shouldn't change after failing overlay" && exit -1
[[ "$(grep "expver=xxxy" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data shouldn't change after failing overlay" && exit -1

# And actually create the overlay

fdb-overlay class=rd,expver=xxxx,stream=oper,date=$YESTERDAY,domain=g,time=0000 \
            class=rd,expver=xxxy,stream=oper,date=$YESTERDAY,domain=g,time=0000 --force

# The overlaid data should now be visible

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "36" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "time=1200" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Data shouldn't change after failing overlay" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=1200" | grep "step=0" | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data shouldn't change after failing overlay" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=0000" | grep "step=0" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Data shouldn't change after failing overlay" && exit -1

exit 0
