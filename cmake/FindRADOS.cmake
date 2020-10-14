# (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

# - Try to find Rados
# Once done this will define
#
#  RADOS_FOUND         - system has Armadillo
#  RADOS_INCLUDE_DIRS  - the Armadillo include directory
#  RADOS_LIBRARIES     - the Armadillo library
#  RADOS_VERSION - This is set to $major.$minor.$patch (eg. 0.9.8)
#
# The following paths will be searched with priority if set in CMake or env
#
#  RADOS_PATH          - prefix path of the Armadillo installation
#  RADOS_ROOT              - Set this variable to the root installation

# Search with priority for RADOS_PATH if given as CMake or env var

find_path(RADOS_INCLUDE_DIR rados/librados.hpp
          HINTS $ENV{RADOS_ROOT} ${RADOS_ROOT}
          PATHS ${RADOS_PATH} ENV RADOS_PATH
          PATH_SUFFIXES include NO_DEFAULT_PATH)

find_path(RADOS_INCLUDE_DIR rados/librados.hpp PATH_SUFFIXES include )

# Search with priority for RADOS_PATH if given as CMake or env var
find_library(RADOS_LIBRARY rados
            HINTS $ENV{RADOS_ROOT} ${RADOS_ROOT}
            PATHS ${RADOS_PATH} ENV RADOS_PATH
            PATH_SUFFIXES lib64 lib NO_DEFAULT_PATH)

find_library( RADOS_LIBRARY rados PATH_SUFFIXES lib64 lib )

set( RADOS_LIBRARIES    ${RADOS_LIBRARY} )
set( RADOS_INCLUDE_DIRS ${RADOS_INCLUDE_DIR} )

include(FindPackageHandleStandardArgs)

# handle the QUIET and REQUIRED arguments and set RADOS_FOUND to TRUE
# if all listed variables are TRUE
# Note: capitalisation of the package name must be the same as in the file name
find_package_handle_standard_args(RADOS DEFAULT_MSG RADOS_LIBRARY RADOS_INCLUDE_DIR)

mark_as_advanced(RADOS_INCLUDE_DIR RADOS_LIBRARY)
