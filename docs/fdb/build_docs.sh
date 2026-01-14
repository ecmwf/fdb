#!/usr/bin/env bash
set -ex

script_dir="$(dirname "$(readlink -f "$0")")"
doxyfile="${script_dir}/Doxyfile"
doxygen_executable="${DOXYGEN_EXECUTABLE:-doxygen}"
sphinx_executable="${SPHINX_EXECUTABLE:-sphinx-build}"
version_str=$(cat "${script_dir}/../../VERSION")
output_dir=${DOCBUILD_OUTPUT:-doc-build}
doxygen_output_dir="${output_dir}/doxygen"
doxygen_input_dir="${script_dir}/../src/fdb5/api"

mkdir -p "${doxygen_output_dir}"

echo "PWD=$(pwd)"

DOXYGEN_OUTPUT_DIR="${doxygen_output_dir}" \
    DOXYGEN_INPUT_DIR="${doxygen_input_dir}" \
    ${doxygen_executable} ${doxyfile}

"${sphinx_executable}" -j auto -E -a -T \
    -Dbreathe_projects.FDB=$(readlink -f ${doxygen_output_dir}/xml) \
    -Dversion=${version_str} \
    -Drelease=${version_str} \
    "${script_dir}" "${output_dir}/sphinx"
