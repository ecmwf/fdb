#!/usr/bin/env bash
# Set up the data for the test

set -euxo pipefail

grib_ls -m data.xxxx.0.grib

# Write simple data

grib2fdb5 -f data.xxxx.0.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Subtocs incorrect" && exit -1

# Duplicating data adds, but not to the counts

grib2fdb5 -f data.xxxx.0.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Subtocs incorrect" && exit -1

# Adding data in the same DB doesn't add more tocs

grib2fdb5 -f data.xxxx.1.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "6" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "6" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Subtocs incorrect" && exit -1

# But we can also add to another DB

grib2fdb5 -f data.xxxy.0.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "36" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "36" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "8" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "8" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Subtocs incorrect" && exit -1

# Adding data in the same DB doesn't add more tocs

grib2fdb5 -f data.xxxy.1.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "96" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "10" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "10" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Subtocs incorrect" && exit -1

exit 0
