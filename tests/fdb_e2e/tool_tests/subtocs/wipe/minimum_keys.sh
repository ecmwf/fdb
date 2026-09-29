#!/usr/bin/env bash
set -euxo pipefail

# Ensure that we can wipe specified databases, but only when fully specified

grib_ls -m data.xxxx.grib

fdb-write data.xxxx.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly duplicated" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly duplicated" && exit -1

# Check that if any of the keys are missing, then nothing happens

for invalid_key in class=rd,expver=xxxx,stream=oper,date=-1 \
                   class=rd,expver=xxxx,stream=oper,time=1200 \
                   class=rd,expver=xxxx,date=-1,time=1200 \
                   class=rd,stream=oper,date=-1,time=1200 \
                   expver=xxxx,stream=oper,date=-1,time=1200
do

  set +e
  fdb-wipe $invalid_key --doit
  rc=$?
  set -e

  [[ $rc = 0 ]] && echo "Should not have succeeded: $invalid_key" && exit 1

  fdb-list --all --full --minimum-keys="" --porcelain | tee out
  [[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "No data should be purged with insufficient keys" && exit -1
  [[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "No data should be purged with insufficient keys" && exit -1
  [[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "No data should be purged with insufficient keys" && exit -1

done

# Check that supplying the key makes it work

fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1,time=1200 --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "12" ]] && echo "Data should be purged when fully specified" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data should be purged when fully specified" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Data should be purged when fully specified" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Data should be purged when fully specified" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data should be purged when fully specified" && exit -1

fdb-wipe class=rd,expver=xxxx,stream=oper,date=-1,time=0000/1200 --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && echo "Data should be purged when fully specified" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data should be purged when fully specified" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data should be purged when fully specified" && exit -1

# Re-trite the data

fdb-write data.xxxx.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1

# Check that we can change the minimum keys

for invalid_key in class=rd,time=1200 \
                   class=rd,expver=xxxx \
                   expver=xxxx,time=1200
do

  set +e
  fdb-wipe --minimum-keys=class,expver,time $invalid_key --doit
  rc=$?
  set -e

  [[ $rc = 0 ]] && echo "Should not have succeeded: $invalid_key" && exit 1

  fdb-list --all --full --minimum-keys="" --porcelain | tee out
  [[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "No data should be purged with insufficient keys" && exit -1
  [[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "No data should be purged with insufficient keys" && exit -1
  [[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "No data should be purged with insufficient keys" && exit -1

done

# And that the purge works now this is supplied

fdb-wipe --minimum-keys=class,expver,time class=rd,expver=xxxx,time=0000/1200 --doit
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && echo "Data should be purged when fully specified" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data should be purged when fully specified" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data should be purged when fully specified" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Data should be purged when fully specified" && exit -1

exit 0
