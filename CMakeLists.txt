############################################################################################
# cmake options:
#
#       -DCMAKE_BUILD_TYPE=Debug|RelWithDebInfo|Release|Production
#       -DCMAKE_INSTALL_PREFIX=/path/to/install
#
#       -DCMAKE_MODULE_PATH=/path/to/ecbuild/cmake
#
#       -DCMAKE_C_COMPILER=gcc
#       -DCMAKE_C_COMPILER=g++
#
#       -DCMAKE_PREFIX_PATH=/path/to/jasper:/path/to/any/package/out/of/place
#       -DBUILD_SHARED_LIBS=OFF
#
#       -DGRIB_API_PATH=/path/to/grib
#       -DHPSS_PATH=/path/to/hpss
#       -DADSM_PATH=/path/to/adsm
#       -DREADLINE_PATH=/path/to/readline
#
#       -DWITH_READLINE=ON          [default: OFF]
#       -DWITH_DUMMY_TAPES=ON       [default: OFF]

cmake_minimum_required( VERSION 2.8.4 FATAL_ERROR )

project( mars_server C CXX )

set( CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH} )

include( ecbuild_system )

if( ECBUILD_MACRO_VERSION VERSION_LESS 1.1 )
	message( FATAL_ERROR "${PROJECT_NAME} needs ecbuild >= 1.1" )
endif()

###############################################################################
# projects

set( PERSISTENT_NAMESPACE "eckit" CACHE INTERNAL "" )

ecbuild_use_package( PROJECT grib_api VERSION 1.10.4 REQUIRED )
ecbuild_use_package( PROJECT eckit    VERSION 0.2    REQUIRED )
ecbuild_use_package( PROJECT odb_api  VERSION 0.10   REQUIRED )
ecbuild_use_package( PROJECT fdb      VERSION 5.0 )

ecbuild_declare_project()

###############################################################################
# some variables of this project

set( MARS_TAPE_MANAGERS  "" CACHE INTERNAL "" )

option( WITH_DUMMY_TAPES "use dummy tape management libraries for testing" OFF )
option( WITH_READLINE    "try to find and use the GNU Readline library"    ON  )

# eventually should be moved out ...
option( BUILD_POINTDB    "either to build pointdb or not"                  ON  )

set( DHSHOME "$ENV{DHSHOME}" )
if( DHSHOME AND NOT DEFINED EC_LINK_DIR )
    set( EC_LINK_DIR ${DHSHOME} )
endif()

###############################################################################
# find extra packages

find_package( Curses  REQUIRED )
find_package( Scin )
find_package( HPSS QUIET )
find_package( ADSM QUIET )

# find readline if user requests readline support
# but don't accept the EditLine wrapper
set( READLINE_WRAPPER_OK FALSE )
if( WITH_READLINE OR DEFINED READLINE_PATH )
    find_package( READLINE )
endif()

###############################################################################
# contents

include_directories( ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_BINARY_DIR}/src )
include_directories( ${ECKIT_INCLUDE_DIRS} )

set_directory_properties( PROPERTIES COMPILE_DEFINITIONS "${ECKIT_DEFINITIONS};${GRIB_API_DEFINITIONS};${ODB_API_DEFINITIONS}" )

# scripts for administration
if( EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/admin/CMakeLists.txt )
	add_subdirectory( admin  )
endif()

ecbuild_add_resources( TARGET          server_resources
					   DONT_PACK_REGEX odb_api/src/cmake_config/configure.*
                       DONT_PACK_DIRS  http etc perl src/client src/pointdb
					   src/sandbox src/fdbsvr )

# scripts for deployment / installation / setup
add_subdirectory( scripts )

# source files
add_subdirectory( src )

# build directory and scripts for developers
if( EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/build/CMakeLists.txt )
	add_subdirectory( build )
endif()

############################################################################################
# finalize

ecbuild_install_project( NAME MarsServer )

ecbuild_print_summary()

