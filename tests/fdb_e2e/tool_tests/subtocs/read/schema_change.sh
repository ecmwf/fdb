#!/usr/bin/env bash
set -euxo pipefail

TODAY=$1
YESTERDAY=$2
BEFORE_YESTERDAY=$3
BEFORE_BEFORE_YESTERDAY=$4

# Change the schema
# i) Ensure that appropriate copies of the schemas are taken
# ii) Ensure that we can successfully write
# iii) Ensure that we can successfully read

grib_ls -m data.xxxx.0.grib

# Rather than using the default schema
# TODO(TKR) The copied schema from /home/fdbdev/etc/fdb/schema is read only and therefore
# we are getting permission denied issues here

cat > ./etc/fdb/schema <<EOF
[ class, expver, stream=oper/dcda/scda, date, time, domain
       [ type, levtype
               [ step, levelist?, param ]]
]
EOF

# Write data with original schema

fdb-write data.xxxx.-3.grib
fdb-write data.xxxx.-2.grib

# Test that the schemas are correctly stored

find . -name schema | grep 0000 | grep $BEFORE_BEFORE_YESTERDAY > find.1
[[ "$(wc -l < find.1 | tr -d '[:space:]')" != "1" ]] && echo "Incorrect schema count" && exit -1
cmp $(cat find.1) ./etc/fdb/schema

# Change the schema. Change it in such a way that it actually makes a difference for this data!

cat > ./etc/fdb/schema <<EOF
[ class, expver, stream=oper/dcda/scda, date, time, domain
       [ type
               [ levtype, step, levelist?, param ]]
]
EOF

# Write data with new schema

fdb-write data.xxxx.-1.grib
fdb-write data.xxxx.0.grib

# Test that the schemas are correctly stored

find . -name schema | grep 0000 | grep $YESTERDAY > find.2
[[ "$(wc -l < find.2 | tr -d '[:space:]')" != "1" ]] && echo "Incorrect schema count" && exit -1
cmp $(cat find.2) ./etc/fdb/schema

# The DBs should have differing schemas

set +e
cmp $(cat find.1) $(cat find.2)
rc=$?
set -e
[[ $rc = 0 ]] && echo "Stored schemas should differ" && exit -1

# Test that we have the data we are expecting

fdb-list --all --minimum-keys="" --porcelain | tee out
[[ "$(wc -l < out | tr -d '[:space:]')" != "96" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "date=$TODAY" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "date=$YESTERDAY" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "date=$BEFORE_YESTERDAY" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "date=$BEFORE_BEFORE_YESTERDAY" out | wc -l | tr -d '[:space:]')" != "24" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "date=$TODAY" out | grep "time=1200" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1
[[ "$(grep "date=$TODAY" out | grep "time=0000" | wc -l | tr -d '[:space:]')" != "12" ]] && echo "Incorrect number of entries written" && exit -1

# Check that we can read data from before the change

# cat > req.xxxx.-3 << EOF
# retrieve,{{ SAMPLE_REQUEST | FLATTEN_MARS_REQUEST({"class": "rd", "expver": "xxxx", "date": (DATE-(3*DAY))|LONG_DATE, "domain": "g"}) }}
# EOF
fdb-read req.xxxx.-3 out.xxxx.-3.grib
grib_compare out.xxxx.-3.grib data.xxxx.-3.grib

# Check that we can read data from after the change

# cat > req.xxxx.0 << EOF
# retrieve,{{ SAMPLE_REQUEST | FLATTEN_MARS_REQUEST({"class": "rd", "expver": "xxxx", "date": DATE|LONG_DATE, "domain": "g"}) }}
# EOF
fdb-read req.xxxx.0 out.xxxx.0.grib
grib_compare out.xxxx.0.grib data.xxxx.0.grib

# Check that we can read

# cat > req.combined << EOF
# retrieve,{{ SAMPLE_REQUEST | FLATTEN_MARS_REQUEST({"class": "rd", "expver": "xxxx", "date": [(DATE-(3*DAY))|LONG_DATE, (DATE-(2*DAY))|LONG_DATE, (DATE-DAY)|LONG_DATE,DATE|LONG_DATE], "domain": "g"}) }}
# EOF
cat data.xxxx.{-{3,2,1},0}.grib > data.xxxx.combined.grib
fdb-read req.xxxx.combined out.xxxx.combined.grib
grib_compare out.xxxx.combined.grib data.xxxx.combined.grib

exit 0

