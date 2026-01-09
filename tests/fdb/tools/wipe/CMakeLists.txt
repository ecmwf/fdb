# get test data for the wipe tests
# ecbuild_get_test_multidata( TARGET get_fdb_test_data_wipe LABELS wipe NAMES "od.oper.grib" )

# list of tests
list( APPEND fdb_tools_wipe_tests
    check
)

# add the tests
foreach( _test ${fdb_tools_wipe_tests} )

    configure_file( wipe_${_test}.sh.in wipe_${_test}.sh @ONLY )

    ecbuild_add_test(
        TARGET test_fdb_tools_wipe_${_test}
        TYPE SCRIPT
        LABELS wipe
        CONDITION HAVE_FDB_BUILD_TOOLS
        TEST_DEPENDS fdb5_get_test_data
        COMMAND wipe_${_test}.sh
        ENVIRONMENT "${test_environment}" )

endforeach( )
