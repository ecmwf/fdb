#!/usr/bin/env bash

yell() { echo "$(basename "$0"): $*" >&2; }
die() { yell "$*"; exit 1; }
try() { "$@" || die "Errored HERE => '$*'"; }

export FDB_DIR="~/dev/bundle_stack/fdb"

export PATH="$FDB_DIR/bin:$PATH"
# export FDB_HOME="$FDB_DIR/build"
export FDB5_CONFIG_FILE="local.yaml"

REQUEST='class=rd,expver=wxyz,stream=enfo,date=20240902,time=0000,domain=g,type=pf,levtype=ml,step=9,number=9,levelist=9,param=1'

########################################################################################################################

try time fdb-inspect $REQUEST

try xctrace record --template 'Metins Instrument' --launch -- $FDB_DIR/bin/fdb-inspect $REQUEST
