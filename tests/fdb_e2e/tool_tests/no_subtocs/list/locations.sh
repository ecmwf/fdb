#!/usr/bin/env bash
# Ensure that listing finds the correct data, but excludes duplicates.
set -euxo pipefail

# Check that we can obtain the location of the data
grib_ls -m data.xxxx.grib
fdb-write data.xxxx.grib

fdb-list --all --minimum-keys="" --porcelain --full | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1

# Specify the location flag

fdb-list class=rd,expver=xxxx,time=0000 --porcelain --location | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "12" ]] && echo "Incorrect data found" && exit -1
[[ "$(grep "root/rd:xxxx" out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Locations not reported" && exit -1

# And without the location flag, none of the location info is included

fdb-list class=rd,expver=xxxx,time=0000 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "12" ]] && echo "Incorrect data found" && exit -1
[[ "$(grep "root/rd:xxxx" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Locations incorrectly reported" && exit -1

exit 0
