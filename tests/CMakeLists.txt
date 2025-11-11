# Unit tests

# If eccodes MEMFS is disabled, set the definition and samples path 
# env variables for all tests, picking the values defined for these 
# paths in the eccodes CMakeLists.txt

set( test_environment "" )

if (NOT eccodes_HAVE_MEMFS)
    list( APPEND test_environment 
        ECCODES_DEFINITION_PATH=${ECCODES_DEFINITION_PATH}
        ECCODES_SAMPLES_PATH=${ECCODES_SAMPLES_PATH})
endif()

include_directories( ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} )

add_subdirectory( fdb )
add_subdirectory( chunked_data_view )
add_subdirectory( regressions )

if (HAVE_PYTHON_ZARR_INTERFACE)
    add_subdirectory( z3fdb )
    add_subdirectory( pychunked_data_view )
endif()

if (HAVE_PYTHON_FDB_INTERFACE)
    add_subdirectory( pyfdb )
endif()
