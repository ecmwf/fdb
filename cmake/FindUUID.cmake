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

# uuid

find_path(UUID_INCLUDE_DIR
    NAMES uuid/uuid.h
    HINTS
        ${UUID_ROOT}
        ${UUID_DIR}
        ${UUID_PATH}
        ENV UUID_ROOT
        ENV UUID_DIR
        ENV UUID_PATH
    PATH_SUFFIXES include include/uuid
    NO_DEFAULT_PATH
)

find_library(UUID_LIBRARY
    NAMES uuid
    HINTS
        ${UUID_ROOT}
        ${UUID_DIR}
        ${UUID_PATH}
        ENV UUID_ROOT
        ENV UUID_DIR
        ENV UUID_PATH
    PATH_SUFFIXES lib lib64
)

find_package_handle_standard_args(UUID DEFAULT_MSG UUID_LIBRARY UUID_INCLUDE_DIR)

mark_as_advanced(UUID_INCLUDE_DIR UUID_LIBRARY)

if(UUID_FOUND)
    add_library(uuid UNKNOWN IMPORTED GLOBAL)
    set_target_properties(uuid PROPERTIES
        IMPORTED_LOCATION ${UUID_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${UUID_INCLUDE_DIR}
    )
endif()
