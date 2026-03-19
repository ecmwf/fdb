#!/usr/bin/env bash
# Set up the data for the test

set -euxo pipefail

YESTERDAY="$1"

grib_ls -m data.xxxx.0.grib

regex_x0="{class=rd,expver=xxxx,stream=oper,date=[0-9]+,time=(12|00)00,domain=g}{type=fc,levtype=pl}{step=0,levelist=[0-9]+,param=[0-9]+}"
regex_x1="{class=rd,expver=xxxx,stream=oper,date=[0-9]+,time=(12|00)00,domain=g}{type=fc,levtype=pl}{step=1,levelist=[0-9]+,param=[0-9]+}"

fdb-write data.xxxx.0.grib
fdb-write data.xxxx.1.grib

fdb-list --all --minimum-keys="" --porcelain --full | tee out
[[ "$(wc -l < out| tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_x0" out | wc -l| tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_x1" out | wc -l| tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1

# Hide some of the data
fdb-hide class=rd,expver=xxxx,stream=oper,domain=g,date=$YESTERDAY,time=1200 --doit

fdb-list --all --minimum-keys="" --porcelain --full | tee out
[[ "$(wc -l < out| tr -d '[:space:]')" != "24" ]] && echo "Unexpected visible data" && exit -1

# When we do fdb-wipe, we also wipe the hidden data

[[ "$(find . -name toc | wc -l| tr -d '[:space:]')" != "2" ]] && "Incorrect data present" && exit -1
[[ "$(find . -name "*.data" | wc -l| tr -d '[:space:]')" != "4" ]] && "Incorrect data present" && exit -1
[[ "$(find . -name "*.index" | wc -l| tr -d '[:space:]')" != "4" ]] && "Incorrect data present" && exit -1

fdb-wipe class=rd,expver=xxxx --minimum-keys="" --doit

[[ "$(find . -name toc | wc -l| tr -d '[:space:]')" != "0" ]] && echo "All data should have been wiped" && exit -1
[[ "$(find . -name "*.data" | wc -l| tr -d '[:space:]')" != "0" ]] && echo "All data should have been wiped" && exit -1
[[ "$(find . -name "*.index" | wc -l| tr -d '[:space:]')" != "0" ]] && echo "All data should have been wiped" && exit -1

exit 0
