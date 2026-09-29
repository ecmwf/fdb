#!/usr/bin/env bash
set -euxo pipefail

# If we specify such that we select only within a given index, we should never delete data.

grib_ls -m data.xxxx.grib

fdb-write data.xxxx.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Data not correctly written" && exit -1

set +e
fdb-wipe --minimum-keys=class,expver class=rd,expver=xxxx,levelist=300 --doit
rc=$?
set -e
[[ $rc = 0 ]] && echo "Should not have succeeded with overspecified key." && exit 1

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Nothing should change when overspecified" && exit -1

fdb-wipe --minimum-keys=class,expver class=rd,expver=xxxx --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && echo "Full purge should occur when not overspecified" && exit -1

exit 0
