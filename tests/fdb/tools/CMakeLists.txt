list( APPEND fdb_tools_tests
    fdb_info )

foreach( _t ${fdb_tools_tests} )
    
    configure_file( ${_t}.sh.in ${_t}.sh @ONLY )

    ecbuild_add_test(
        TYPE SCRIPT
        COMMAND ${_t}.sh)

endforeach()
