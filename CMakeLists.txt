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

set( CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild/cmake")

include( ecbuild_system )

ecbuild_requires_macro_version( 1.3 )

###############################################################################
# projects

set( PERSISTENT_NAMESPACE "eckit" CACHE INTERNAL "" )

ecbuild_use_package( PROJECT grib_api VERSION 1.10  REQUIRED )
ecbuild_use_package( PROJECT eckit    VERSION 0.6   REQUIRED )
ecbuild_use_package( PROJECT odb_api  VERSION 0.10  REQUIRED )

option( ENABLE_FDBSVR "build the fdb server" ON  )
if( ENABLE_FDBSVR )
	ecbuild_use_package( PROJECT fdb VERSION 5.1 )
	if( NOT FDB_FOUND )
		set( ENABLE_FDBSVR OFF )
	endif()
endif()

ecbuild_declare_project()

###############################################################################
# some variables of this project

set( MARS_TAPE_MANAGERS  "" CACHE INTERNAL "" )

option( ENABLE_READLINE     "try to find and use the GNU Readline library"    ON  )

option( ENABLE_CLIENT       "build the client prototype"                      ON  )

option( ENABLE_DUMMY_TAPES  "use dummy tape management libraries for testing" OFF )

ecbuild_add_option( FEATURE SANDBOX
					DEFAULT OFF
					DESCRIPTION "build the sandbox stuff" )

set( DHSHOME "$ENV{DHSHOME}" )
if( DHSHOME AND NOT DEFINED EC_LINK_DIR )
    set( EC_LINK_DIR ${DHSHOME} )
endif()

###############################################################################
# find extra packages

find_package( Curses  REQUIRED )
find_package( HPSS QUIET )
find_package( ADSM QUIET )

# find readline if user requests readline support
# but don't accept the EditLine wrapper
set( READLINE_WRAPPER_OK FALSE )
if( ENABLE_READLINE OR DEFINED READLINE_PATH )
    find_package( READLINE )
endif()

# find thread library ( preferably pthreads )

set( CMAKE_THREAD_PREFER_PTHREAD 1 )
find_package(Threads REQUIRED)

###############################################################################
# checks

# check thread library is pthreads

if( NOT ${CMAKE_USE_PTHREADS_INIT} )
	message( FATAL_ERROR "Only pthreads supported - thread library found is [${CMAKE_THREAD_LIBS_INIT}]" )
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
					   SOURCES_PACK    INSTALL LICENSE
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

ecbuild_install_project( NAME mars-server )

ecbuild_print_summary()

