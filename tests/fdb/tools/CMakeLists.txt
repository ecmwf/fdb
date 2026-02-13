list( APPEND fdb_tools_tests
    fdb_info )

foreach( _t ${fdb_tools_tests} )

    configure_file( ${_t}.sh.in ${_t}.sh @ONLY )

    ecbuild_add_test(
        TYPE SCRIPT
        CONDITION HAVE_FDB_BUILD_TOOLS
        COMMAND ${_t}.sh
        ENVIRONMENT "${test_environment}")

endforeach()

list( APPEND fdb_tools_grib_tests
    fdb_axes )

foreach( _t ${fdb_tools_grib_tests} )
    
    configure_file( ${_t}.sh.in ${_t}.sh @ONLY )

    ecbuild_add_test(
        TYPE SCRIPT
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB
        ENVIRONMENT ECCODES_BINARY_PATH=$<TARGET_FILE_DIR:grib_set>
        ENVIRONMENT "${test_environment}"
        COMMAND ${_t}.sh)

endforeach()

add_subdirectory( auxiliary )
add_subdirectory( list )
add_subdirectory( compare )
add_subdirectory( inspect )
add_subdirectory( reindex )
add_subdirectory( dump-toc )
add_subdirectory( copy )

