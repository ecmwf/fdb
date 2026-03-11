list( APPEND fdb_tools_tests
    info )

foreach( _t ${fdb_tools_tests} )

    configure_file( fdb_${_t}.sh.in fdb_${_t}.sh @ONLY )

    ecbuild_add_test(
        TARGET fdb_test_tools_${_t}
        TYPE SCRIPT
        COMMAND fdb_${_t}.sh
        ENVIRONMENT "${test_environment}")

endforeach()

list( APPEND fdb_tools_grib_tests
    axes )

foreach( _t ${fdb_tools_grib_tests} )

    configure_file( fdb_${_t}.sh.in fdb_${_t}.sh @ONLY )

    ecbuild_add_test(
        TARGET fdb_test_tools_${_t}
        TYPE SCRIPT
        CONDITION HAVE_GRIB
        ENVIRONMENT ECCODES_BINARY_PATH=$<TARGET_FILE_DIR:grib_set>
        ENVIRONMENT "${test_environment}"
        COMMAND fdb_${_t}.sh)

endforeach()

add_subdirectory( auxiliary )
add_subdirectory( list )
# add_subdirectory( compare )
add_subdirectory( inspect )
add_subdirectory( wipe )
add_subdirectory( reindex )
add_subdirectory( dump-toc )
add_subdirectory( copy )

