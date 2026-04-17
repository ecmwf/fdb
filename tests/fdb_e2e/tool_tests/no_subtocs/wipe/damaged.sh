#!/usr/bin/env bash
set -euxo pipefail
# Ensure that we can wipe specified databases

grib_ls -m data.xxxx.grib

fdb-write data.xxxx.grib

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect files present" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect files present" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Incorrect files present" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Incorrect files present" && exit -1

rm -rf $(find . -name '*.data')
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Failed to nuke data files" && exit -1

# Check that the wipe command still works!

fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1,time=0000/1200 --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1
[[ "$(find . -name '*.index.extra' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1

exit 0
