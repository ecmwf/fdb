cmake_minimum_required( VERSION 3.18 FATAL_ERROR )

find_package( ecbuild 3.12 REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild)

project( fdb5 LANGUAGES C CXX )

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_COMPILE_WARNING_AS_ERROR ON)
# add_compile_options(-fsanitize=address)
# add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
# add_link_options(-fsanitize=address)
# set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wno-unused-parameter -Wno-unused-variable -Wno-sign-compare")
# set(CMAKE_CXX_FLAGS "-Wno-unused-parameter -Wno-unused-variable -Wno-reorder -Wno-sign-compare -Wvla-cxx-extension")

CHECK_TYPE_SIZE(off_t SIZEOF_OFF_T)

if ("${SIZEOF_OFF_T}" LESS 8)
    MESSAGE(SEND_ERROR "Missing support for large files (off_t size < 64 bits)")
endif()

########################################################################################################################
### dependencies and options

### Documentation
ecbuild_add_option( FEATURE FDB_DOCUMENTATION
                    DEFAULT OFF
                    DESCRIPTION "Enable documentation generation" )

### eckit

set( PERSISTENT_NAMESPACE "eckit" CACHE INTERNAL "" ) # needed for generating .b files for persistent support

ecbuild_find_package( NAME eckit  VERSION 1.32 REQUIRED )

### GRIB support

ecbuild_find_package( NAME eccodes VERSION 2.10 QUIET )

ecbuild_add_option( FEATURE GRIB
                    DEFAULT ON
                    CONDITION eccodes_FOUND
                    DESCRIPTION "Support for GRIB via eccodes")

ecbuild_find_package( NAME metkit VERSION 1.15 REQUIRED )

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

find_package(LibUUID QUIET)

find_package(DAOS QUIET)


set(_default_dummy_daos ON)
if(DAOS_FOUND)
    set(_default_dummy_daos OFF)
endif()

ecbuild_add_option( FEATURE DUMMY_DAOS
                    DEFAULT ${_default_dummy_daos}
                    CONDITION LibUUID_FOUND
                    DESCRIPTION "Use dummy DAOS library emulating DAOS with a file system" )

if(HAVE_DUMMY_DAOS)
    set(DAOS_LIBRARIES dummy_daos)
    set(DAOS_INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/src/dummy_daos)
    set(DAOS_FOUND TRUE)
    set(DAOS_TESTS_LIBRARIES dummy_daos_tests)
    set(DAOS_TESTS_INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/src/dummy_daos)
    set(DAOS_TESTS_FOUND TRUE)
endif()

ecbuild_add_option( FEATURE DAOSFDB
                    CONDITION DAOS_FOUND AND LibUUID_FOUND
                    DEFAULT ON
                    DESCRIPTION "DAOS support for FDB Store" )

ecbuild_add_option( FEATURE DAOS_ADMIN
                    DEFAULT ${_default_dummy_daos}
                    CONDITION HAVE_DAOSFDB AND DAOS_TESTS_FOUND
                    DESCRIPTION "Add features for DAOS pool management. Removes need to manually create a pool for DAOS unit tests" )

# DAOS_TESTS is a daos admin library, usually not present
if(NOT HAVE_DAOS_ADMIN)
    set( DAOS_TESTS_LIBRARIES "" )
    set( DAOS_TESTS_INCLUDE_DIRS "" )
endif()

ecbuild_add_option( FEATURE EXPERIMENTAL
                    DEFAULT OFF
                    DESCRIPTION "Experimental features" )

ecbuild_add_option( FEATURE SANDBOX
                    DESCRIPTION "build the sandbox stuff"
                    DEFAULT OFF	 )

ecbuild_add_option( FEATURE PYTHON_ZARR_INTERFACE
                    DESCRIPTION "Build python Zarr interface layer"
                    DEFAULT OFF
                    REQUIRED_PACKAGES "NAME pybind11 VERSION 3.0.1" )

ecbuild_add_option( FEATURE PYTHON_FDB_INTERFACE
                    DESCRIPTION "Build python FDB interface layer"
                    DEFAULT OFF
                    REQUIRED_PACKAGES "NAME pybind11 VERSION 3.0.1" )

if(HAVE_PYTHON_ZARR_INTERFACE OR HAVE_PYTHON_FDB_INTERFACE)
    find_package(Python 3.11 COMPONENTS Interpreter Development REQUIRED)
endif()


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
if(ENABLE_FDB_DOCUMENTATION)
    add_subdirectory(docs)
endif()


ecbuild_install_project( NAME fdb )

ecbuild_print_summary()
