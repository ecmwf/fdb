# unit tests

# if eccodes MEMFS is disabled, set the definition and samples path 
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
add_subdirectory( regressions )

