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

# Changed expver forbidden

set +e
fdb-overlay --variable-keys="class" \
            class=rd,expver=xxxx,stream=oper,date=$YESTERDAY,domain=g,time=0000 \
            class=rd,expver=xxxy,stream=oper,date=$YESTERDAY,domain=g,time=0000
rc=$?
set -e
[[ $rc = 0 ]] && echo "fdb-overlay should not succeed with invalid keys" && exit 1

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "No additional data added with non-permitted keys" && exit -1

# Changed class forbidden

set +e
fdb-overlay --variable-keys="expver" \
            class=rd,expver=xxxx,stream=oper,date=$YESTERDAY,domain=g,time=0000 \
            class=od,expver=xxxy,stream=oper,date=$YESTERDAY,domain=g,time=0000
rc=$?
set -e
[[ $rc = 0 ]] && echo "fdb-overlay should not succeed with invalid keys" && exit 1

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "No additional data added with non-permitted keys" && exit -1

# And now with all the permitted keys

fdb-overlay --variable-keys="class,expver" \
            class=rd,expver=xxxx,stream=oper,date=$YESTERDAY,domain=g,time=0000 \
            class=rd,expver=xxxy,stream=oper,date=$YESTERDAY,domain=g,time=0000

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "36" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "time=1200" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxx" out | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=1200" | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "expver=xxxy" out | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1

exit 0
