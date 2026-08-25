#!/usr/bin/env bash
# Set up the data for the test

set -euxo pipefail

# This is the same as simple.sh, but it sets some options that should have no effect

grib_ls -m data.xxxx.0.grib

# Write simple data

grib2fdb5 -d -1 -f data.xxxx.0.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out| tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l| tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l| tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l| tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
# TODO(TKR) [[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "{% if USE_SUBTOCS %}4{% else %}2{% endif %}" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l| tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
# TODO(TKR) [[ "$(find . -name 'toc.*' | wc -l| tr -d '[:space:]')" != "{% if USE_SUBTOCS %}2{% else %}0{% endif %}" ]] && echo "Subtocs incorrect" && exit -1
[[ "$(find . -name 'toc.*' | wc -l| tr -d '[:space:]')" != "0" ]] && echo "Subtocs incorrect" && exit -1

# Duplicating data adds, but not to the counts

grib2fdb5 -d -1 -f data.xxxx.0.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out| tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l| tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l| tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l| tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
# TODO(TKR) [[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "{% if USE_SUBTOCS %}8{% else %}4{% endif %}" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l| tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l| tr -d '[:space:]')" != "0" ]] && echo "Subtocs incorrect" && exit -1
# TODO(TKR) [[ "$(find . -name 'toc.*' | wc -l| tr -d '[:space:]')" != "{% if USE_SUBTOCS %}4{% else %}0{% endif %}" ]] && echo "Subtocs incorrect" && exit -1

# Adding data in the same DB doesn't add more tocs

grib2fdb5 -d -1 -f data.xxxx.1.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out| tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l| tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l| tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l| tr -d '[:space:]')" != "6" ]] && echo "Data not correctly written" && exit -1
#  TODO(TKR)[[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "{% if USE_SUBTOCS %}12{% else %}6{% endif %}" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "6" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l| tr -d '[:space:]')" != "2" ]] && echo "Data not correctly written" && exit -1
# TODO(TKR) [[ "$(find . -name 'toc.*' | wc -l| tr -d '[:space:]')" != "{% if USE_SUBTOCS %}6{% else %}0{% endif %}" ]] && echo "Subtocs incorrect" && exit -1
[[ "$(find . -name 'toc.*' | wc -l| tr -d '[:space:]')" != "0" ]] && echo "Subtocs incorrect" && exit -1

# But we can also add to another DB

grib2fdb5 -d -1 -f data.xxxy.0.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out| tr -d '[:space:]')" != "72" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l| tr -d '[:space:]')" != "36" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l| tr -d '[:space:]')" != "36" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l| tr -d '[:space:]')" != "8" ]] && echo "Data not correctly written" && exit -1
# TODO(TKR) [[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "{% if USE_SUBTOCS %}16{% else %}8{% endif %}" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "8" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l| tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
# TODO(TKR) [[ "$(find . -name 'toc.*' | wc -l| tr -d '[:space:]')" != "{% if USE_SUBTOCS %}8{% else %}0{% endif %}" ]] && echo "Subtocs incorrect" && exit -1
[[ "$(find . -name 'toc.*' | wc -l| tr -d '[:space:]')" != "0" ]] && echo "Subtocs incorrect" && exit -1

# Adding data in the same DB doesn't add more tocs

grib2fdb5 -d -1 -f data.xxxy.1.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out| tr -d '[:space:]')" != "96" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=1200 out | wc -l| tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep time=0000 out | wc -l| tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(find . -name '*.data' | wc -l| tr -d '[:space:]')" != "10" ]] && echo "Data not correctly written" && exit -1
# TODO(TKR) [[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "{% if USE_SUBTOCS %}20{% else %}10{% endif %}" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name '*.index' | wc -l| tr -d '[:space:]')" != "10" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc' | wc -l| tr -d '[:space:]')" != "4" ]] && echo "Data not correctly written" && exit -1
[[ "$(find . -name 'toc.*' | wc -l| tr -d '[:space:]')" != "0" ]] && echo "Subtocs incorrect" && exit -1
# TODO(TKR) [[ "$(find . -name 'toc.*' | wc -l| tr -d '[:space:]')" != "{% if USE_SUBTOCS %}10{% else %}0{% endif %}" ]] && echo "Subtocs incorrect" && exit -1

exit 0
