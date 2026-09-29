#!/usr/bin/env bash
# Ensure that listing finds the correct data, but excludes duplicates.
set -euxo pipefail

grib_ls -m data.xxxx.0.grib

# Set up some regexes for later testing

regex_x0="{class=rd,expver=xxxx,stream=oper,date=[0-9]+,time=(12|00)00,domain=g}{type=fc,levtype=pl}{step=0,levelist=[0-9]+,param=[0-9]+}"
regex_x1="{class=rd,expver=xxxx,stream=oper,date=[0-9]+,time=(12|00)00,domain=g}{type=fc,levtype=pl}{step=1,levelist=[0-9]+,param=[0-9]+}"
regex_x2="{class=rd,expver=xxxx,stream=oper,date=[0-9]+,time=(12|00)00,domain=g}{type=fc,levtype=pl}{step=2,levelist=[0-9]+,param=[0-9]+}"
regex_y0="{class=rd,expver=xxxy,stream=oper,date=[0-9]+,time=(12|00)00,domain=g}{type=fc,levtype=pl}{step=0,levelist=[0-9]+,param=[0-9]+}"
regex_y1="{class=rd,expver=xxxy,stream=oper,date=[0-9]+,time=(12|00)00,domain=g}{type=fc,levtype=pl}{step=1,levelist=[0-9]+,param=[0-9]+}"
regex_y2="{class=rd,expver=xxxy,stream=oper,date=[0-9]+,time=(12|00)00,domain=g}{type=fc,levtype=pl}{step=2,levelist=[0-9]+,param=[0-9]+}"

# We loop over all these tests twice. We should get the same listing results both times
# as the default behaviour is to only show the data that would be retrieved (i.e. that
# that matches a MARS request.

for i in 1 2;
do


fdb-write data.xxxx.0.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
if [[ "$i" = "1" ]]; then
  [[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_x1" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_x2" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_y0" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_y1" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_y2" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
fi
[[ "$(grep -E "$regex_x0" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1


fdb-write data.xxxy.0.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
if [[ "$i" = "1" ]]; then [[ "$(wc -l < out | tr -d '[:space:]')" != "48" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_x1" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_x2" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_y1" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_y2" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
fi
[[ "$(grep -E "$regex_x0" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_y0" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1


fdb-write data.xxxx.1.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
if [[ "$i" = "1" ]]; then
  [[ "$(wc -l < out | tr -d '[:space:]')" != "72" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_x2" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_y1" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_y2" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
fi
[[ "$(grep -E "$regex_x0" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_x1" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_y0" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1



fdb-write data.xxxy.1.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
if [[ "$i" = "1" ]]; then
  [[ "$(wc -l < out | tr -d '[:space:]' | tr -d '[:space:]')" != "96" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_x2" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_y2" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
fi
[[ "$(grep -E "$regex_x0" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_x1" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_y0" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_y1" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1


fdb-write data.xxxx.2.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
if [[ "$i" = "1" ]]; then
  [[ "$(wc -l < out | tr -d '[:space:]')" != "120" ]] && echo "Incorrect number of entries written" && exit -1
  [[ "$(grep -E "$regex_y2" out | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries written" && exit -1
fi
[[ "$(grep -E "$regex_x0" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_x1" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_x2" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_y0" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_y1" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1


fdb-write data.xxxy.2.grib
fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "144" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_x0" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_x1" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_x2" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_y0" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_y1" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E "$regex_y2" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1


# End loop with duplicate writes

done

exit 0

