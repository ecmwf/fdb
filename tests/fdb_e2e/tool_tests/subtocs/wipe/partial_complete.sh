#!/usr/bin/env bash
set -euxo pipefail

# Ensure that we can purge a subset of the data within the DB, which
# is indexed according to different second level metadata

grib_ls -m data.xxxx.an.grib
grib_ls -m data.xxxx.fc.grib

fdb-write data.xxxx.an.grib
fdb-write data.xxxx.fc.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Data not correctly written" && exit -1

fdb-wipe --minimum-keys=class,expver class=rd,expver=xxxx,type=fc
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "No change should be made without --doit" && exit -1

fdb-wipe --minimum-keys=class,expver class=rd,expver=xxxx,type=fc --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep type=fc out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(grep type=an out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Incorrect amount of data removed" && exit -1

exit 0
