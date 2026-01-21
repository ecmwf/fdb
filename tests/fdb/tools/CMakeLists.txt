list( APPEND fdb_tools_tests
    fdb_info
    fdb_axes )

foreach( _t ${fdb_tools_tests} )
    
    configure_file( ${_t}.sh.in ${_t}.sh @ONLY )

    ecbuild_add_test(
        TYPE SCRIPT
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB
        ENVIRONMENT ECCODES_BINARY_PATH=$<TARGET_FILE_DIR:grib_set>
        COMMAND ${_t}.sh)

endforeach()

add_subdirectory( auxiliary )
