#!/bin/bash
set -e -x

# Builds the content to be copied into the z3fd-web-service container
# Will build pakages for:
#  - z3fdb wheel
#  - z3fdb-web-service wheel
#  - fdb
#
# Expects the following pre-conditions:
#  - Working directory for this build contains all sources layed out like this
#  .
#  ├── cxx-dependencies
#  ├── ecbuild
#  ├── eccodes
#  ├── eckit
#  ├── fdb
#  └── metkit

require_dir() {
    local dir="${1:?Directory name required}"
    [[ -d "$dir" ]] || { echo "Error: directory '$dir' not found" >&2; exit 1; }
}

require_dirs() {
    local dir
    for dir in "$@"; do
        require_dir "$dir"
    done
}

link_dirs() {
    local base_path="${1:?Base path required}"
    shift
    [[ $# -eq 0 ]] && { echo "Error: no directories specified" >&2; exit 1; }

    local name
    for name in "$@"; do
        ln -s "${base_path}/${name}" "./${name}" || { echo "Failed to link: $name" >&2; exit 1; }
    done
}

# Ensure pre-requisites are meet
deps_repo="cxx-dependencies"

required_repos=(
    "ecbuild"
    "eccodes"
    "eckit"
    "metkit"
    "fdb"
)

require_dirs "${required_repos[@]}" "$deps_repo"

# Locations
root="$(pwd)"
stage="$root"/stage
deps="$stage"/deps
deps_build="$stage"/deps_build
src="$stage"/src
build="$stage"/build
install="$stage"/install/fdb

# Build and install third-party-dependencies
BUILD_PATH="$deps_build" INSTALL_PREFIX="$deps" cxx-dependencies/build.sh

# Isolate sources from dependencies install-location and create bundle cmake
mkdir -p "$src"
cd "$src"
link_dirs "$root" "${required_repos[@]}"
cat << 'EOF' > CMakeLists.txt
cmake_minimum_required(VERSION 3.12 FATAL_ERROR)
find_package(ecbuild 3.0 REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR}/ecbuild)
set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "")
set(ECKIT_ENABLE_AEC ON CACHE STRING "")
set(ECCODES_ENABLE_AEC ON CACHE STRING "")
set(ECCODES_ENABLE_MEMFS ON CACHE STRING "")
set(FDB5_ENABLE_PYTHON_ZARR_INTERFACE ON CACHE STRING "")
project(z3fdb-web-service VERSION 1.0.0)
add_subdirectory(eccodes)
add_subdirectory(eckit)
add_subdirectory(metkit)
add_subdirectory(fdb)
EOF

# Compile & test
cd "$root"
mkdir -p "$install"
mkdir -p "$build"
cmake \
    -B "$build" \
    -S "$src" \
    -GNinja \
    -DCMAKE_INSTALL_PREFIX="$install" \
    -DCMAKE_PREFIX_PATH="$deps"
cmake --build "$build" -j
cd "$build"
# skip eckit_test_utils_optional (Failling on GCC 15)
ctest -j $(nproc) --output-on-failure -E eckit_test_utils_optional
cd "$root"
cmake --build "$build" -j -t install
cp "$deps"/lib/libaec* "$install"/lib
for lib in "$install"/lib/*.so*; do patchelf --set-rpath '$ORIGIN' "$lib"; done
rm -rf $install/include
tar --zstd -cvf fdb.tar.zst -C "$install" ..
mv "$build"/*.whl "$root"
python -m build --wheel "$src"/fdb/tools/z3fdb-web-service -o .
