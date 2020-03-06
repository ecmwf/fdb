cmake_minimum_required( VERSION 3.1 FATAL_ERROR )

project( fdb5 C CXX )

set( CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild/cmake")

include( ecbuild_system NO_POLICY_SCOPE )

ecbuild_requires_macro_version( 2.9 )

ecbuild_declare_project()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

########################################################################################################################
### dependencies and options

### eckit

set( PERSISTENT_NAMESPACE "eckit" CACHE INTERNAL "" ) # needed for generating .b files for persistent support

ecbuild_use_package( PROJECT eckit  VERSION 1.7 REQUIRED )

### GRIB support

ecbuild_use_package( PROJECT eccodes VERSION 2.10 REQUIRED )

ecbuild_use_package( PROJECT metkit VERSION 1.4 REQUIRED ) ## metkit is now always needed

### FDB remote front-end

ecbuild_add_option( FEATURE FDB_REMOTE
                    DEFAULT ON
                    DESCRIPTION "Support distributed remote FDB" )

### FDB backend in persistent memory, i.e. pmem (NVRAM)

ecbuild_add_option( FEATURE PMEMFDB # option present in fdb5_config.h
                    DEFAULT OFF
                    DESCRIPTION "Persistent memory (NVRAM) support for FDB"
                    REQUIRED_PACKAGES "PROJECT pmem" )

### FDB backend in CEPH object store (using Rados)

ecbuild_add_option( FEATURE RADOSFDB  # option defined in fdb5_config.h
                    DEFAULT ON
                    DESCRIPTION "Ceph/Rados support for FDB Store"
                    REQUIRED_PACKAGES Rados )

### FDB backend in indexed filesystem with table-of-contents, i.e. TOC
### Supports Lustre parallel filesystem stripping control

ecbuild_add_option( FEATURE TOCFDB  # option defined in fdb5_config.h
                    DEFAULT ON
                    DESCRIPTION "Filesystem TOC support for FDB" )

find_package( LustreAPI QUIET )

ecbuild_add_option( FEATURE LUSTRE  # option defined in fdb5_config.h
                    CONDITION LUSTREAPI_FOUND
                    DEFAULT ON
                    DESCRIPTION "Filesystem TOC support for FDB" )

### experimental & sandbox features

ecbuild_add_option( FEATURE EXPERIMENTAL
                    DEFAULT OFF
                    DESCRIPTION "Experimental features" )

ecbuild_add_option( FEATURE SANDBOX
                    DESCRIPTION "build the sandbox stuff"
                    DEFAULT OFF	 )

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

set( FDB5_INCLUDE_DIRS  ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_BINARY_DIR}/src )

include_directories( ${FDB5_INCLUDE_DIRS} )
include_directories( ${ECKIT_INCLUDE_DIRS} )
include_directories( ${METKIT_INCLUDE_DIRS} )

set_directory_properties( PROPERTIES COMPILE_DEFINITIONS "${ECKIT_DEFINITIONS};${ECCODES_DEFINITIONS};${ODB_API_DEFINITIONS}" )

get_directory_property( FDB5_DEFINITIONS COMPILE_DEFINITIONS )

### source files

add_subdirectory( src )
add_subdirectory( tests )

### finalize

ecbuild_install_project( NAME fdb )

ecbuild_print_summary()
