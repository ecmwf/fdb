#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p localroot

export FDB_HOME="${FDB_HOME:-$(cd ../build/fdb5 && pwd)}"
export FDB5_CONFIG_FILE="$PWD/local.yaml"
export FDB_PIMS_HACKAHON=1
export PIMS_ENVIRONMENT="${PIMS_ENVIRONMENT:-demo}"
export PIMS_HOST="${PIMS_HOST:-localhost}"
export PIMS_PORT="${PIMS_PORT:-9999}"
# The bundled gribjump plugin needs rebuilding after updating metkit.
export AUTO_LOAD_PLUGINS=0

../build/bin/fdb-write test1.grib test2.grib
