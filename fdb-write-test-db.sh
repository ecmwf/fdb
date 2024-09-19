#!/usr/bin/env bash

yell() { echo "$(basename "$0"): $*" >&2; }
die() { yell "$*"; exit 1; }
try() { "$@" || die "Errored HERE => '$*'"; }

export FDB_DIR=~/dev/bundle_stack/fdb

# export PATH=~/workspace/bundle_stack/build/bin:$PATH
# export ECCODES_DEFINITION_PATH=~/workspace/bundle_stack/build/share/eccodes/definitions
# export FDB_HOME="$FDB_DIR/build"
export FDB5_CONFIG_FILE="local.yaml"

DB_NAME=large_fdb_test
BASE_GRIB=test.grib

########################################################################################################################

# rm -rf "./$DB_NAME" || true
# mkdir -p "./$DB_NAME/localroot"

try cd ./$DB_NAME

try cp "$FDB_DIR/$BASE_GRIB" .
try cp "$FDB_DIR/build/etc/fdb/schema" .
try cp "$FDB_DIR/tests/fdb/tools/list/local.yaml" .

########################################################################################################################

echo "Populating testing FDB using $BASE_GRIB"

for i in $(seq -f "%02g" 1 2); do
  # echo "number $i"
  try grib_set -s date=202409"$i" $BASE_GRIB tmp.grib

  # fdb-hammer --class=od --expver=0001 --nlevels=5 --nparams=5 --nensembles=10 --nsteps=10 tmp.grib
  # fdb-hammer --class=od --expver=xxxx --nlevels=5 --nparams=5 --nensembles=10 --nsteps=10 tmp.grib
  # fdb-hammer --class=od --expver=wxyz --nlevels=5 --nparams=5 --nensembles=10 --nsteps=10 tmp.grib

  fdb-hammer --class=rd --expver=0001 --nlevels=5 --nparams=5 --nensembles=10 --nsteps=10 tmp.grib
  fdb-hammer --class=rd --expver=xxxx --nlevels=5 --nparams=5 --nensembles=10 --nsteps=10 tmp.grib
  fdb-hammer --class=rd --expver=wxyz --nlevels=5 --nparams=5 --nensembles=10 --nsteps=10 tmp.grib

#   fdb-hammer --class=ea --expver=0001 --nlevels=5 --nparams=5 --nensembles=10 --nsteps=10 tmp.grib
#   fdb-hammer --class=ea --expver=xxxx --nlevels=5 --nparams=5 --nensembles=10 --nsteps=10 tmp.grib
#   fdb-hammer --class=ea --expver=wxyz --nlevels=5 --nparams=5 --nensembles=10 --nsteps=10 tmp.grib

done
