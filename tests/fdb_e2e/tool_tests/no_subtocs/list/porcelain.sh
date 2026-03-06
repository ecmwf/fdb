#!/usr/bin/env bash
set -euxo pipefail

# If we don't specify --porcelain, then the expanded request is prepended to the output request.
# Measure the difference!

grib_ls -m data.xxxx.grib
fdb-write data.xxxx.grib

fdb-list --all --minimum-keys="" --porcelain --full | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1

# With normal output

fdb-list class=rd,expver=xxxx | tee out
[[ "$(grep '{class=rd,expver=xxxx' out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries reported" && exit -1
[[ "$(grep '^list,$' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "MARS request incorrectly reported" && exit -1
[[ "$(grep '^[	].*=.*' out | wc -l | tr -d '[:space:]')" != "2" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep '^[	]class=rd' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "MARS request incorrectly reported" && exit -1
[[ "$(grep '^[	]expver=xxxx' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "MARS request incorrectly reported" && exit -1


# With porcelain output

fdb-list class=rd,expver=xxxx --porcelain | tee out
[[ "$(grep '{class=rd,expver=xxxx' out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries reported" && exit -1
[[ "$(wc -l < out | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries reported" && exit -1

# Test with expansion of date and param without porcelain

fdb-list class=rd,expver=xxxx,date=-1,stream=oper,type=an,levtype=pl,param=t | tee out
[[ "$(grep '{class=rd,expver=xxxx' out | grep -E 'date=[0-9]{8}' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries reported" && exit -1
[[ "$(grep '^list,$' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries reported" && exit -1
[[ "$(grep '^[	].*=.*' out | wc -l | tr -d '[:space:]')" != "7" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep '^[	]class=rd' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep '^[	]expver=xxxx' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep '^[	]param=130' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E '^[	]date=[0-9]{8}' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1

# Test with expansion of date and param with porcelain

fdb-list class=rd,expver=xxxx,date=-1,stream=oper,type=an,levtype=pl,param=t --porcelain | tee out
[[ "$(grep '{class=rd,expver=xxxx' out | grep -E 'date=[0-9]{8}' | wc -l | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries reported" && exit -1
[[ "$(wc -l < out | tr -d '[:space:]')" != "0" ]] && echo "Incorrect number of entries reported" && exit -1

# Test with expansion of date and param without porcelain

fdb-list class=rd,expver=xxxx,date=-1,stream=oper,type=an,levtype=pl,param=138 | tee out
[[ "$(grep '{class=rd,expver=xxxx' out | grep -E 'date=[0-9]{8}' | grep param=138 | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries reported" && exit -1
[[ "$(grep '^list,$' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries reported" && exit -1
[[ "$(grep '^[	].*=.*' out | wc -l | tr -d '[:space:]')" != "7" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep '^[	]class=rd' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep '^[	]expver=xxxx' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep '^[	]param=138' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep -E '^[	]date=[0-9]{8}' out | wc -l | tr -d '[:space:]')" != "1" ]] && echo "Incorrect number of entries written" && exit -1

# Test with expansion of date and param with porcelain

fdb-list class=rd,expver=xxxx,date=-1,stream=oper,type=an,levtype=pl,param=138 --porcelain | tee out
[[ "$(grep '{class=rd,expver=xxxx' out | grep -E 'date=[0-9]{8}' | grep param=138 | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries reported" && exit -1
[[ "$(wc -l < out | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries reported" && exit -1

exit 0
