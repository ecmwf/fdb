#!/usr/bin/env bash
set -euxo pipefail

# Do a partial wipe that deletes all of the data

grib_ls -m data.xxxx.an.grib

fdb-write data.xxxx.an.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Data not correctly written" && exit -1

fdb-wipe --minimum-keys=class,expver class=rd,expver=xxxx,type=an
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "No change should be made without --doit" && exit -1

fdb-wipe --minimum-keys=class,expver class=rd,expver=xxxx,type=an --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1
[[ "$(find root -name 'schema' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect amount of data removed" && exit -1

exit 0
