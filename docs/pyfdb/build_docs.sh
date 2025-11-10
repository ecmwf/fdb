#!/usr/bin/env bash
set -ex

script_dir="$(dirname "$(readlink -f "$0")")"
sphinx_executable="${SPHINX_EXECUTABLE:-sphinx-build}"
version_str=$(cat "${script_dir}/../../VERSION")
output_dir=${DOCBUILD_OUTPUT:-doc-build}

echo "PWD=$(pwd)"

"${sphinx_executable}" -j auto -E -a -T \
    -Dversion=${version_str} \
    -Drelease=${version_str} \
    "${script_dir}" "${output_dir}/sphinx"

