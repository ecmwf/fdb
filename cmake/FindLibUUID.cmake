# Copyright 2024- European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
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
#
# Requires:
#  FindPackageHandleStandardArgs (CMake standard module)
#

#[=======================================================================[.rst:
FindLibUUID
--------

This module finds the libuuid (from util-linux) library.

Imported Targets
^^^^^^^^^^^^^^^^

This module provides the following imported targets, if found:

``LibUUID``
  The libuuid library

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``LIB_UUID_FOUND``
  True if the libuuid library is found.
``LIB_UUID_INCLUDE_DIRS``
  Include directories needed to use libuuid.
``LIB_UUID_LIBRARIES``
  Libraries needed to link to libuuid.

Cache variables
^^^^^^^^^^^^^^^

The following cache variables may also be set to help find libuuid library:

``LIB_UUID_INCLUDE_DIR``
  where to find the libuuid headers.
``LIB_UUID_LIBRARY``
  where to find the libuuid library.

Hints
^^^^^

The environment variables ``LIB_UUID_ROOT``, ``LIB_UUID_DIR``, and ``LIB_UUID_PATH``
may also be set to help find libuuid library.

#]=======================================================================]

find_path(LIB_UUID_INCLUDE_DIR uuid.h
    HINTS
        ${LIB_UUID_ROOT}
        ${LIB_UUID_DIR}
        ${LIB_UUID_PATH}
        ENV LIB_UUID_ROOT
        ENV LIB_UUID_DIR
        ENV LIB_UUID_PATH
    PATH_SUFFIXES uuid
)

find_library(LIB_UUID_LIBRARY
    NAMES uuid
    HINTS
        ${LIB_UUID_ROOT}
        ${LIB_UUID_DIR}
        ${LIB_UUID_PATH}
        ENV LIB_UUID_ROOT
        ENV LIB_UUID_DIR
        ENV LIB_UUID_PATH
    PATH_SUFFIXES lib lib64
)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(LibUUID REQUIRED_VARS
                                    LIB_UUID_LIBRARY
                                    LIB_UUID_INCLUDE_DIR)

if (LibUUID_FOUND)
    set(LIB_UUID_LIBRARIES ${LIB_UUID_LIBRARY})
    set(LIB_UUID_INCLUDE_DIRS ${LIB_UUID_INCLUDE_DIR})
    if(NOT TARGET LibUUID)
        add_library(LibUUID UNKNOWN IMPORTED)
        set_target_properties(LibUUID PROPERTIES
            IMPORTED_LOCATION "${LIB_UUID_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${LIB_UUID_INCLUDE_DIR}"
            INTERFACE_LINK_LIBRARIES "${LIB_UUID_LIBRARY}")
    endif()
endif()

mark_as_advanced(LIB_UUID_INCLUDE_DIR LIB_UUID_LIBRARY)
