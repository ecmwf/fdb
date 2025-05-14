#!/usr/bin/env bash
set -e

doxygen_executable="${DOXYGEN_EXECUTABLE:-doxygen}"
sphinx_executable="${SPHINX_EXECUTABLE:-sphinx-build}"
script_dir="$(dirname "$(readlink -f "$0")")"
doxyfile="${script_dir}/Doxyfile"
output_dir=${DOCBUILD_OUTPUT:-doc-build}
doxygen_output_dir="${output_dir}/doxygen"
doxygen_input_dir="${script_dir}/../src/fdb5/api"
version_str=$(cat "${script_dir}/../VERSION")

mkdir -p "${doxygen_output_dir}"

DOXYGEN_OUTPUT_DIR="${doxygen_output_dir}" \
    DOXYGEN_INPUT_DIR="${doxygen_input_dir}" \
    ${doxygen_executable} ${doxyfile}

pushd "${output_dir}"
"${sphinx_executable}" -b html -j auto \
    -Dbreathe_projects.FDB=${doxygen_output_dir}/xml \
    -Dversion=${version_str} \
    -Drelease=${version_str} \
    "${script_dir}" "sphinx"
popd
