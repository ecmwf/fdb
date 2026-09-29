#!/usr/bin/env bash
set -euxo pipefail

# If we specify such that we select only within a given index, we should never delete data.

grib_ls -m data.xxxx.grib

fdb-write data.xxxx.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Data not correctly written" && exit -1

fdb-write data.xxxx.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Data not correctly duplicated" && exit -1

# TODO: We may in future support this type of purging. If so, remove the type=an
# test, and consider the tests in partial.sh

for over in "type=an" "levelist=300"
do

set +e
fdb-purge --minimum-keys=class,expver class=rd,expver=xxxx,$over --doit
rc=$?
set -e
[[ $rc = 0 ]] && echo "Purging with overspecified key should not have succeeded" && exit 1
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Nothing should change when overspecified" && exit -1

done

fdb-purge --minimum-keys=class,expver class=rd,expver=xxxx --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Full purge should occur when not overspecified" && exit -1

exit 0
