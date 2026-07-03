# (C) Copyright 2026- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#
# This module defines the following variables:
#  RADOS_INCLUDE_DIRS - Where to find rados/librados.h
#  RADOS_LIBRARIES    - The libraries needed to use Rados
#  RADOS_FOUND        - True if Rados was found
#
# This module also defines the following IMPORTED target:
#  Ceph::rados

# Find the header path by looking for the subdirectory file
find_path(RADOS_INCLUDE_DIR
    NAMES rados/librados.h
    DOC "Path to Rados include directory"
)

# Find the library
find_library(RADOS_LIBRARY
    NAMES rados
    DOC "Path to Rados library"
)

# Handle the QUIETLY and REQUIRED arguments and set RADOS_FOUND to TRUE if
# all listed variables are TRUE.
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RADOS
    REQUIRED_VARS RADOS_LIBRARY RADOS_INCLUDE_DIR
)

message(STATUS "DEBUG: RADOS_INCLUDE_DIR = ${RADOS_INCLUDE_DIR}")
message(STATUS "DEBUG: RADOS_LIBRARY     = ${RADOS_LIBRARY}")

if(RADOS_FOUND)
    set(RADOS_LIBRARIES ${RADOS_LIBRARY})
    set(RADOS_INCLUDE_DIRS ${RADOS_INCLUDE_DIR})

    # Create an modern generic imported target
    if(NOT TARGET Ceph::RADOS)
        add_library(Ceph::RADOS UNKNOWN IMPORTED)
        set_target_properties(Ceph::RADOS PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${RADOS_INCLUDE_DIRS}"
            IMPORTED_LOCATION "${RADOS_LIBRARY}"
        )
    endif()
endif()

# Hide these variables from the GUI cache view
mark_as_advanced(RADOS_INCLUDE_DIR RADOS_LIBRARY)
