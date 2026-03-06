#!/usr/bin/env bash
set -euxo pipefail

grib_ls -m data.xxxx.0.grib

# Write simple data

fdb-write data.xxxx.0.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Subtocs incorrect" && exit -1

# Duplicating data adds, but not to the counts

fdb-write data.xxxx.0.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "8" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Subtocs incorrect" && exit -1

# Adding data in the same DB doesn't add more tocs

fdb-write data.xxxx.1.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "6" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l | tr -d '[:space:]')" != "6" ]] && echo "Subtocs incorrect" && exit -1

# But we can also add to another DB

fdb-write data.xxxy.0.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "36" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "36" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "8" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "16" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l | tr -d '[:space:]')" != "8" ]] && echo "Subtocs incorrect" && exit -1

# Adding data in the same DB doesn't add more tocs

fdb-write data.xxxy.1.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "96" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l | tr -d '[:space:]')" != "10" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l | tr -d '[:space:]')" != "20" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l | tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l | tr -d '[:space:]')" != "10" ]] && echo "Subtocs incorrect" && exit -1

exit 0
