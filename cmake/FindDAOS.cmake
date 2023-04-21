# Copyright 2022 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

include(FindPackageHandleStandardArgs)

# daos

find_path(DAOS_INCLUDE_DIR
    NAMES daos.h
    HINTS
        ${DAOS_ROOT}
        ${DAOS_DIR}
        ${DAOS_PATH}
        ENV DAOS_ROOT
        ENV DAOS_DIR
        ENV DAOS_PATH
    PATH_SUFFIXES include
)

find_library(DAOS_LIBRARY
    NAMES daos
    HINTS
        ${DAOS_ROOT}
        ${DAOS_DIR}
        ${DAOS_PATH}
        ENV DAOS_ROOT
        ENV DAOS_DIR
        ENV DAOS_PATH
    PATH_SUFFIXES lib lib64
)

# daos_common

find_library(DAOS_COMMON_LIBRARY
    NAMES daos_common
    HINTS
        ${DAOS_ROOT}
        ${DAOS_DIR}
        ${DAOS_PATH}
        ENV DAOS_ROOT
        ENV DAOS_DIR
        ENV DAOS_PATH
    PATH_SUFFIXES lib lib64
)

# gurt

find_library(GURT_LIBRARY
    NAMES gurt
    HINTS
        ${DAOS_ROOT}
        ${DAOS_DIR}
        ${DAOS_PATH}
        ENV DAOS_ROOT
        ENV DAOS_DIR
        ENV DAOS_PATH
    PATH_SUFFIXES lib lib64
)

# daos tests

# if not using dummy DAOS, then the daos_tests lib should be installed
# from daos RPMs into the corresponding system directories. However its
# headers (daos/tests_lib.h) are not provided in any of the RPMs and 
# need to be copied from source. tests_lib.h could be copied into standard 
# system directories together with other DAOS headers, but the tests_lib.h
# file includes many other DAOS headers not provided by RPMs and other 
# external library headers, e.g. boost, and are required when compiling a 
# program which includes tests_lib.h (e.g. fdb5's DAOS backend with 
# DAOS_ADMIN enabled for unit testing). To avoid having to install all 
# these headers, tests_lib.h can be copied in a user directory and modified
# to only declare the necessary functions (pool create and destroy) and 
# remove most of the includes. All this explains why a DAOS_TESTS_INCLUDE_ROOT
# environment variable is used here to find tests_lib.h rather than DAOS_ROOT.

find_path(DAOS_TESTS_INCLUDE_DIR
    NAMES daos/tests_lib.h
    HINTS
        ${DAOS_TESTS_INCLUDE_ROOT}
        ${DAOS_TESTS_DIR}
        ${DAOS_TESTS_PATH}
        ENV DAOS_TESTS_INCLUDE_ROOT
        ENV DAOS_TESTS_DIR
        ENV DAOS_TESTS_PATH
    PATH_SUFFIXES include
)

find_library(DAOS_TESTS_LIBRARY
    NAMES daos_tests
    HINTS
        ${DAOS_ROOT}
        ${DAOS_DIR}
        ${DAOS_PATH}
        ENV DAOS_ROOT
        ENV DAOS_DIR
        ENV DAOS_PATH
    PATH_SUFFIXES lib lib64
)

find_package_handle_standard_args(
    DAOS
    DEFAULT_MSG
    DAOS_LIBRARY
    DAOS_INCLUDE_DIR
    DAOS_COMMON_LIBRARY
    GURT_LIBRARY )

mark_as_advanced(DAOS_INCLUDE_DIR DAOS_LIBRARY DAOS_COMMON_LIBRARY GURT_LIBRARY)

if(DAOS_FOUND)
    add_library(daos UNKNOWN IMPORTED GLOBAL)
    set_target_properties(daos PROPERTIES
        IMPORTED_LOCATION ${DAOS_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${DAOS_INCLUDE_DIR}
    )
    add_library(daos_common UNKNOWN IMPORTED GLOBAL)
    set_target_properties(daos_common PROPERTIES
        IMPORTED_LOCATION ${DAOS_COMMON_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${DAOS_INCLUDE_DIR}
    )
    add_library(gurt UNKNOWN IMPORTED GLOBAL)
    set_target_properties(gurt PROPERTIES
        IMPORTED_LOCATION ${GURT_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${DAOS_INCLUDE_DIR}
    )
    set(DAOS_INCLUDE_DIRS ${DAOS_INCLUDE_DIR})
    list(APPEND DAOS_LIBRARIES daos daos_common gurt)
endif()

find_package_handle_standard_args(
    DAOS_TESTS
    DEFAULT_MSG
    DAOS_TESTS_LIBRARY
    DAOS_TESTS_INCLUDE_DIR )

mark_as_advanced(DAOS_TESTS_INCLUDE_DIR DAOS_TESTS_LIBRARY)

if(DAOS_TESTS_FOUND)
    add_library(daos_tests UNKNOWN IMPORTED GLOBAL)
    set_target_properties(daos_tests PROPERTIES
        IMPORTED_LOCATION ${DAOS_TESTS_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES "${DAOS_TESTS_INCLUDE_DIR};${DAOS_INCLUDE_DIR}"
    )
    list(APPEND DAOS_TESTS_INCLUDE_DIRS 
        ${DAOS_TESTS_INCLUDE_DIR}
        ${DAOS_INCLUDE_DIR}
    )
    set(DAOS_TESTS_LIBRARIES daos_tests)
endif()
