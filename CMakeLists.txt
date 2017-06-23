########################################################################################################################
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
#       -DHPSS_PATH=/path/to/hpss
#       -DADSM_PATH=/path/to/adsm

cmake_minimum_required( VERSION 2.8.11 FATAL_ERROR )

project( mars_server C CXX )

set( CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild/cmake")

include( ecbuild_system NO_POLICY_SCOPE )

ecbuild_requires_macro_version( 1.9 )

ecbuild_declare_project()

# Enable C++11 if it is available. We should have alternative code paths for where it is not.
# This is used for performance in fdb-stats (checking for duplicates in a large set).
#
# Use the test:
# #if __cplusplus > 199711L
#    c++11 code
# #else
#    c++03 code
# #endif

ecbuild_add_cxx11_flags()

########################################################################################################################
# sanity checks

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

########################################################################################################################
# project dependencies

### eckit

set( PERSISTENT_NAMESPACE "eckit" CACHE INTERNAL "" ) # needed for generating .b files for persistent support

ecbuild_use_package( PROJECT eckit    VERSION 0.18.0  REQUIRED )

### GRIB support

ecbuild_add_option( FEATURE ECCODES DESCRIPTION "" DEFAULT OFF )

if( HAVE_ECCODES )
  ecbuild_use_package( PROJECT eccodes VERSION 0.16 REQUIRED )
  set( GRIB_API_INCLUDE_DIRS ${ECCODES_INCLUDE_DIRS} )
  set( GRIB_API_LIBRARIES    ${ECCODES_LIBRARIES} )
  set( GRIB_API_DEFINITIONS  ${ECCODES_DEFINITIONS} )
  set( grib_api_BASE_DIR     ${eccodes_BASE_DIR} )
  set( GRIB_API_FOUND        1 )  # Temporary hack
  set( grib_handling_pkg eccodes )
else()
  ecbuild_use_package( PROJECT grib_api VERSION 1.10 REQUIRED )
  set( grib_handling_pkg grib_api )
endif()

### third party archive support

set(mars_2_mars_default OFF)
if(METKIT_FOUND)
  set(mars_2_mars_default ON)
endif()

ecbuild_add_option( FEATURE MARS2MARS
                    DEFAULT ${mars_2_mars_default}
                    DESCRIPTION "Support direct archiving from another mars instance"
                    REQUIRED_PACKAGES "PROJECT metkit VERSION 0.2" )

### ODB support

ecbuild_add_option( FEATURE ODB
                    DEFAULT ON
                    DESCRIPTION "Support for ODB handlers"
                    REQUIRED_PACKAGES "PROJECT odb_api  VERSION 0.10" )

if( HAVE_ODB )
  set( odblib Odb )
  set( odbtoolslib odbtools )
endif()

### FDB backend in persistent memory, i.e. pmem (NVRAM)

ecbuild_add_option( FEATURE PMEMFDB
                    DEFAULT OFF
                    DESCRIPTION "Persistent memory (NVRAM) support for FDB"
                    REQUIRED_PACKAGES "PROJECT pmem" )

### FDB backend in indexed filesystem with table-of-contents, i.e. TOC
### Supports Lustre parallel filesystem stripping control

ecbuild_add_option( FEATURE TOCFDB
                    DEFAULT ON
                    DESCRIPTION "Filesystem TOC support for FDB" )

########################################################################################################################
# project vars & options

set( MARS_TAPE_MANAGERS  "" CACHE INTERNAL "" )

set( DHSHOME "$ENV{DHSHOME}" )
if( DHSHOME AND NOT DEFINED EC_LINK_DIR )
	set( EC_LINK_DIR ${DHSHOME} )
endif()

### server mode
# builds binaries for mars-server
# otherwise skip them for when MARS is built within ifs-support for FDB

ecbuild_add_option( FEATURE MARS_SERVER
                    DEFAULT ON
                    DESCRIPTION "Whether or not to build the Mars Server executables" )

### pointdb

ecbuild_add_option( FEATURE POINTDB
                    DEFAULT ON
                    DESCRIPTION "Whether or not to build the PointDB -- [EXPERIMENTAL]"
                    REQUIRED_PACKAGES "PROJECT metkit VERSION 0.2" )

### fdb

ecbuild_add_option( FEATURE     FDB
                    DEFAULT     ON
                    DESCRIPTION "Whether or not to build the FDB -- [EXPERIMENTAL]" )

### dummy tapes stubs to build when no tape library exists (for development only)

ecbuild_add_option( FEATURE DUMMY_TAPES
                    DESCRIPTION "use dummy tape management libraries for testing"
                    DEFAULT OFF )

#set( NETCDF_CXX TRUE )
#ecbuild_add_option( FEATURE NETCDF
#	DEFAULT ON
#	DESCRIPTION "support for NetCDF"
#	REQUIRED_PACKAGES NetCDF )

ecbuild_add_option( FEATURE SANDBOX
                    DESCRIPTION "build the sandbox stuff"
                    DEFAULT OFF	 )

########################################################################################################################
# find extra packages

find_package( HPSS QUIET )

find_package( ADSM QUIET )

find_package( LustreAPI QUIET )

# find thread library ( preferably pthreads )

set( CMAKE_THREAD_PREFER_PTHREAD 1 )
find_package(Threads REQUIRED)

########################################################################################################################
# checks

# check thread library is pthreads

if( NOT ${CMAKE_USE_PTHREADS_INIT} )
	message( FATAL_ERROR "Only pthreads supported - thread library found is [${CMAKE_THREAD_LIBS_INIT}]" )
endif()

########################################################################################################################
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
add_subdirectory( regressions )

############################################################################################
# finalize

ecbuild_install_project( NAME mars-server )

ecbuild_print_summary()

