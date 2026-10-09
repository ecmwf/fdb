# (C) Copyright 2026- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

# pyfdb on its own, against an installed fdb:
#   cmake -S python/pyfdb -B build -DCMAKE_PREFIX_PATH=<fdb and its deps>

cmake_minimum_required( VERSION 3.18 FATAL_ERROR )

project( pyfdb LANGUAGES CXX C )

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package( fdb5 REQUIRED )
find_package( Python 3.11 REQUIRED COMPONENTS Interpreter Development )
find_package( pybind11 3.0.1 REQUIRED )

set( fdb5_VERSION_STR ${fdb5_VERSION} )

include( ${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/pyfdb.cmake )

enable_testing()
add_subdirectory( ${CMAKE_CURRENT_SOURCE_DIR}/../../tests/pyfdb ${CMAKE_CURRENT_BINARY_DIR}/tests )
