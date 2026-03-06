#!/usr/bin/env bash
set -euxo pipefail
#
# Ensure that we can wipe specified databases

grib_ls -m data.xxxx.grib

fdb-write data.xxxx.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect files present" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect files present" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Incorrect files present" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Incorrect files present" && exit -1

# Create some additional files

for f in $(find . -name '*.index')
do
  touch $f.extra
done

[[ "$(find . -name '*.index.extra' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Extra files created incorrectly" && exit -1


set +e
fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1,time=0000/1200 --doit
rc=$?
set -e

[[ $rc = 0 ]] && echo "fdb-wipe should not succeed with unexpected data present" && exit 1

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Nothing should be removed with extra data" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Nothing should be removed with extra data" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Nothing should be removed with extra data" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Nothing should be removed with extra data" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Nothing should be removed with extra data" && exit -1
[[ "$(find . -name '*.index.extra' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Nothing should be removed with extra data" && exit -1


# And forcibly remove everything

fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1,time=0000/1200 --doit --unsafe-wipe-all

fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1
[[ "$(find . -name '*.index.extra' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data not fully removed" && exit -1

exit 0
