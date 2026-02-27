#!/usr/bin/env bash
set -ex

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
version_str=$(cat "${repo_root}/VERSION")
output_dir=${DOCBUILD_OUTPUT:-doc-build}
doxygen_output_dir="${output_dir}/doxygen"
doxygen_input_dir="${repo_root}/src/fdb5/api"

doxygen_executable="${DOXYGEN_EXECUTABLE:-doxygen}"
sphinx_executable="${SPHINX_EXECUTABLE:-sphinx-build}"

# Substitute CMake @VARIABLE@ placeholders in Doxyfile.in
mkdir -p "${doxygen_output_dir}"
sed \
    -e "s|@DOXYGEN_OUTPUT_DIR@|${doxygen_output_dir}|g" \
    -e "s|@DOXYGEN_INPUT_DIR@|${doxygen_input_dir}|g" \
    -e "s|@FDB_VERSION@|${version_str}|g" \
    "${script_dir}/Doxyfile.in" > "${output_dir}/Doxyfile"

${doxygen_executable} "${output_dir}/Doxyfile"

"${sphinx_executable}" -j auto -E -a -T \
    -Dbreathe_projects.FDB="$(cd "${doxygen_output_dir}/xml" && pwd)" \
    -Dversion="${version_str}" \
    -Drelease="${version_str}" \
    "${script_dir}" "${output_dir}/sphinx"
