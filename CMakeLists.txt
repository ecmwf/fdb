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

cmake_minimum_required( VERSION 2.8.4 FATAL_ERROR )

project( mars_server C CXX )

set( CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild/cmake")

include( ecbuild_system NO_POLICY_SCOPE )

ecbuild_requires_macro_version( 1.9 )

### off_t must be 64 bits exactly

# test by c compilation
ecbuild_check_c_source_return( "#include <stdio.h>\n#include <sys/types.h>\nint main(){printf(\"%ld\",sizeof(off_t));return 0;}" VAR  check_off_t_final OUTPUT __sizeof_off_t_final )
if( NOT check_off_t_final OR NOT __sizeof_off_t_final EQUAL "8" )
		message( FATAL_ERROR "operating system ${CMAKE_SYSTEM} (${EC_OS_BITS} bits) and sizeof off_t [${__sizeof_off_t_final}] -- off_t must be 64 bits exactly" )
endif()

# test by cxx compilation
ecbuild_check_cxx_source_return( "#include <iostream>\n#include <sys/types.h>\nint main(){ std::cout << sizeof(off_t) << std::endl; return 0; }" VAR  cxx_check_off_t_final OUTPUT __cxx_sizeof_off_t_final )
if( NOT cxx_check_off_t_final OR NOT __cxx_sizeof_off_t_final EQUAL "8" )
		message( FATAL_ERROR "operating system ${CMAKE_SYSTEM} (${EC_OS_BITS} bits) and sizeof off_t [${__sizeof_off_t_final}] -- off_t must be 64 bits exactly" )
endif()

# test by header define (done by ecbuild system)
if( NOT EC_SIZEOF_OFF_T EQUAL "8" )
		message( FATAL_ERROR "operating system ${CMAKE_SYSTEM} (${EC_OS_BITS} bits) and sizeof off_t [${EC_SIZEOF_OFF_T}] -- off_t must be 64 bits exactly" )
endif()

###############################################################################
# project dependencies

set( PERSISTENT_NAMESPACE "eckit" CACHE INTERNAL "" )

ecbuild_use_package( PROJECT grib_api VERSION 1.10  REQUIRED )
ecbuild_use_package( PROJECT eckit    VERSION 0.10  REQUIRED )

### ODB support

ecbuild_add_option( FEATURE ODB
                    DEFAULT ON
                    DESCRIPTION "Support for ODB handlers"
                    REQUIRED_PACKAGES "PROJECT odb_api  VERSION 0.10" )

if( HAVE_ODB )
    set( odblib Odb )
    set( odbtoolslib odbtools )
endif()

###############################################################################
# project vars & options

ecbuild_declare_project()

# some variables

set( MARS_TAPE_MANAGERS  "" CACHE INTERNAL "" )

set( DHSHOME "$ENV{DHSHOME}" )
if( DHSHOME AND NOT DEFINED EC_LINK_DIR )
	set( EC_LINK_DIR ${DHSHOME} )
endif()

### pointdb

ecbuild_add_option( FEATURE POINTDB
					DEFAULT ON
                    DESCRIPTION "Whether or not to build the PointDB -- [EXPERIMENTAL]"
                    REQUIRED_PACKAGES "PROJECT gribpp VERSION 0.1" )

### fdb

ecbuild_add_option( FEATURE     FDB
                    DEFAULT     ON
                    DESCRIPTION "Whether or not to build the FDB -- [EXPERIMENTAL]"
                    REQUIRED_PACKAGES "PROJECT marskit VERSION 0.1" )

ecbuild_add_option( FEATURE DUMMY_TAPES DESCRIPTION "use dummy tape management libraries for testing" DEFAULT OFF )

ecbuild_add_option( FEATURE SANDBOX DEFAULT OFF	DESCRIPTION "build the sandbox stuff" )

###############################################################################
# find extra packages

find_package( HPSS QUIET )

find_package( ADSM QUIET )

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

set( MARS_SERVER_INCLUDE_DIRS  ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_BINARY_DIR}/src )

include_directories( ${MARS_SERVER_INCLUDE_DIRS} )
include_directories( ${ECKIT_INCLUDE_DIRS} )

set_directory_properties( PROPERTIES COMPILE_DEFINITIONS "${ECKIT_DEFINITIONS};${GRIB_API_DEFINITIONS};${ODB_API_DEFINITIONS}" )

get_directory_property( MARS_SERVER_DEFINITIONS COMPILE_DEFINITIONS )

# scripts for administration

if( EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/admin/CMakeLists.txt )
	add_subdirectory( admin  )
endif()

ecbuild_add_resources( TARGET          server_resources
					   SOURCES_PACK    INSTALL LICENSE
					   DONT_PACK_DIRS  http perl src/pointdb src/sandbox )

# scripts for deployment / installation / setup

add_subdirectory( scripts )

# source files

add_subdirectory( src )

# (non-unit) tests

add_subdirectory( tests )

############################################################################################
# finalize

ecbuild_install_project( NAME mars-server )

ecbuild_print_summary()

