cmake_minimum_required( VERSION 3.12 FATAL_ERROR )

find_package( ecbuild 3.7 REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild)

project( fdb5 LANGUAGES C CXX )

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

CHECK_TYPE_SIZE(off_t SIZEOF_OFF_T)

if ("${SIZEOF_OFF_T}" LESS 8)
    MESSAGE(SEND_ERROR "Missing support for large files (off_t size < 64 bits)")
endif()

########################################################################################################################
### dependencies and options

### eckit

set( PERSISTENT_NAMESPACE "eckit" CACHE INTERNAL "" ) # needed for generating .b files for persistent support

ecbuild_find_package( NAME eckit  VERSION 1.24.4 REQUIRED )

### GRIB support

ecbuild_find_package( NAME eccodes VERSION 2.10 QUIET )

ecbuild_add_option( FEATURE GRIB
                    DEFAULT ON
                    CONDITION eccodes_FOUND
                    DESCRIPTION "Support for GRIB via eccodes")

ecbuild_find_package( NAME metkit VERSION 1.5 REQUIRED )

### FDB backend in persistent memory, i.e. pmem (NVRAM)

ecbuild_add_option( FEATURE PMEMFDB # option present in fdb5_config.h
                    DEFAULT OFF
                    DESCRIPTION "Persistent memory (NVRAM) support for FDB"
                    REQUIRED_PACKAGES "NAME pmem" )

### FDB backend in CEPH object store (using Rados)

find_package( RADOS QUIET )
ecbuild_add_option( FEATURE RADOSFDB  # option defined in fdb5_config.h
                    CONDITION eckit_HAVE_RADOS AND RADOS_FOUND
                    DEFAULT OFF
                    DESCRIPTION "Ceph/Rados support for FDB Store" )

### FDB backend in indexed filesystem with table-of-contents, i.e. TOC
### Supports Lustre parallel filesystem stripping control

ecbuild_add_option( FEATURE TOCFDB  # option defined in fdb5_config.h
                    DEFAULT ON
                    DESCRIPTION "Filesystem TOC support for FDB" )

### support for Lustre API control of file stripping

find_package( LUSTREAPI QUIET )

ecbuild_add_option( FEATURE LUSTRE  # option defined in fdb5_config.h
                    CONDITION LUSTREAPI_FOUND
                    DEFAULT ON
                    DESCRIPTION "Support for Lustre API control of file stripping " )

### experimental & sandbox features

ecbuild_add_option( FEATURE FDB_REMOTE
                    DEFAULT ON
                    DESCRIPTION "Support for FDB remote access" )

ecbuild_add_option( FEATURE EXPERIMENTAL
                    DEFAULT OFF
                    DESCRIPTION "Experimental features" )

ecbuild_add_option( FEATURE SANDBOX
                    DESCRIPTION "build the sandbox stuff"
                    DEFAULT OFF	 )

find_package( dhskit QUIET )

ecbuild_add_option( FEATURE HTTPSVR
                    CONDITION dhskit_FOUND
                    DEFAULT OFF
                    DESCRIPTION "httpsvr functionality for fdb5" )

### build the tools

ecbuild_add_option( FEATURE BUILD_TOOLS
                    DEFAULT ON
                    DESCRIPTION "Build the command line tools" )

if(HAVE_BUILD_TOOLS)
    set(_default_fdb_tools ON)
else()
    set(_default_fdb_tools OFF)
endif()

ecbuild_add_option( FEATURE FDB_BUILD_TOOLS
                    DEFAULT ${_default_fdb_tools}
                    DESCRIPTION "Build the command line FDB tools" )

### find thread library ( preferably pthreads )

set( CMAKE_THREAD_PREFER_PTHREAD 1 )
find_package(Threads REQUIRED)

### checks

# check thread library is pthreads

if( NOT ${CMAKE_USE_PTHREADS_INIT} )
	message( FATAL_ERROR "Only pthreads supported - thread library found is [${CMAKE_THREAD_LIBS_INIT}]" )
endif()

########################################################################################################################
# contents

include(cmake/compiler_warnings.cmake) # optionally handle compiler specific warnings

set( fdb5_INCLUDE_DIRS  ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_BINARY_DIR}/src )

include_directories( ${fdb5_INCLUDE_DIRS} )

set_directory_properties( PROPERTIES COMPILE_DEFINITIONS "${ECKIT_DEFINITIONS};${ECCODES_DEFINITIONS}" )

get_directory_property( fdb5_DEFINITIONS COMPILE_DEFINITIONS )

### source files

add_subdirectory( src )
add_subdirectory( tests )

### finalize

ecbuild_install_project( NAME fdb )

ecbuild_print_summary()
