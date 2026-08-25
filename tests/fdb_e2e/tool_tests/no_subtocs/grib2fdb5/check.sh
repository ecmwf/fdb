#!/usr/bin/env bash
# Set up the data for the test

set -euxo pipefail

# Check that the verification keys do what they say they should

grib_ls -m data.xxxx.grib


for invalid_check in "-e xxxy" "-c uk" "-s enfo" "-T fc"
do

    set +e
    grib2fdb5 $invalid_check -f data.xxxx.grib
    rc=$?
    set -e

    [[ $rc = 0 ]] && echo "Should not have succeeded with invalid check: $invalid_check" && exit 1
done


for valid_check in "-e xxxx" "-c rd" "-s oper" "-T an"
do

    # Sanity check
    fdb-list --all --full --minimum-keys="" --porcelain | tee out
    [[ "$(wc -l < out| tr -d '[:space:]')" != "0" ]] && echo "No data should be purged with insufficient keys" && exit -1
    [[ "$(find . -name '*.data' | wc -l| tr -d '[:space:]')" != "0" ]] && echo "No data should be purged with insufficient keys" && exit -1
    [[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "0" ]] && echo "No data should be purged with insufficient keys" && exit -1

    grib2fdb5 $valid_check -f data.xxxx.grib

    fdb-list --all --full --minimum-keys="" --porcelain | tee out
    [[ "$(wc -l < out| tr -d '[:space:]')" != "24" ]] && echo "No data should be purged with insufficient keys" && exit -1
    [[ "$(find . -name '*.data' | wc -l| tr -d '[:space:]')" != "2" ]] && echo "No data should be purged with insufficient keys" && exit -1
    # TODO(TKR)
    # [[ "$(find -name '*.index' | wc -l| tr -d '[:space:]')" != "{% if USE_SUBTOCS %}4{% else %}2{% endif %}" ]] && echo "No data should be purged with insufficient keys" && exit -1
    [[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "2" ]] && echo "No data should be purged with insufficient keys" && exit -1

    # Ensure there is no data left for the next loop
    fdb-wipe --minimum-keys="class,expver" class=rd,expver=xxxx --doit
done

exit 0
