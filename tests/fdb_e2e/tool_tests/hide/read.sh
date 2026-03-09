#!/usr/bin/env bash
# Set up the data for the test

set -euxo pipefail

YESTERDAY="$1"

grib_ls -m data.xxxx.0.grib

regex_x0="{class=rd,expver=xxxx,stream=oper,date=[0-9]+,time=(12|00)00,domain=g}{type=fc,levtype=pl}{step=0,levelist=[0-9]+,param=[0-9]+}"
regex_x1="{class=rd,expver=xxxx,stream=oper,date=[0-9]+,time=(12|00)00,domain=g}{type=fc,levtype=pl}{step=1,levelist=[0-9]+,param=[0-9]+}"

fdb-list --all --minimum-keys="" --porcelain --full | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_x0" out | wc -l| tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_x1" out | wc -l| tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1

# Without --doit, nothing happens

fdb-hide class=rd,expver=xxxx,stream=oper,domain=g,date=$YESTERDAY,time=1200

fdb-read --extract data.xxxx.1.grib out.1.no-doit.grib
grib_compare out.1.no-doit.grib data.xxxx.1.grib

# Hide some of the data

fdb-hide class=rd,expver=xxxx,stream=oper,domain=g,date=$YESTERDAY,time=1200 --doit

fdb-read --extract data.xxxx.1.grib out.1.hidden.grib
set +e
grib_compare out.1.hidden.grib data.xxxx.1.grib
rc=$?
set -e
[[ $rc = 0 ]] && echo "Retrieved data should not match!" && exit -1

grib_copy -w time=0000 data.xxxx.1.grib data.xxxx.1.time0000.grib
grib_compare out.1.hidden.grib data.xxxx.1.time0000.grib

# And we can write data (even to the same DB) after hiding

fdb-write data.xxxx.1.grib

fdb-read --extract data.xxxx.1.grib out.1.rewritten.grib
grib_compare out.1.rewritten.grib data.xxxx.1.grib
