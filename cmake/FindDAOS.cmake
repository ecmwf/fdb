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
endif()
