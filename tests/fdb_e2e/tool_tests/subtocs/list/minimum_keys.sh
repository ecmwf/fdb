#!/usr/bin/env bash
# Ensure that listing finds the correct data, but excludes duplicates.
set -euxo pipefail

# Ensure that we can wipe specified databases, but only when fully specified

grib_ls -m data.xxxx.grib

fdb-write data.xxxx.grib
fdb-list --all --full --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Data not correctly written" && exit -1

# Check that if any of the keys are missing, then nothing happens

for invalid_key in class=rd \
                   expver=xxxx
do
  set +e
  fdb-list $invalid_key
  rc=$?
  set -e

  [[ $rc = 0 ]] && echo "Should not have succeeded: $invalid_key" && exit 1
done

# Check that supplying the key makes it work

fdb-list class=rd,expver=xxxx --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Should work when we specify the keys" && exit -1

# Check that we can change the minimum keys

for invalid_key in class=rd,expver=xxxx \
                   class=rd,time=1200 \
                   expver=xxxx,time=1200
do
  set +e
  fdb-list --minimum-keys=class,expver,time $invalid_key
  rc=$?
  set -e

  [[ $rc = 0 ]] && echo "Should not have succeeded: $invalid_key" && exit 1
done

# And that the purge works now this is supplied

fdb-list --minimum-keys=class,expver,time class=rd,expver=xxxx,time=1200 --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "12" ]] && echo "Should work when we specify the keys" && exit -1

exit 0
