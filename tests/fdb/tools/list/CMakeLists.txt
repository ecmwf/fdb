# get test data for the list tests
ecbuild_get_test_multidata( TARGET get_fdb_test_data_list LABELS list NAMES "od.oper.grib" NOCHECK )

# list of tests
list( APPEND fdb_tools_list_tests
    all
    all_full
    location
    masking
    minimum_keys
    porcelain
    ranges
    depth
)

# add the tests
foreach( _test ${fdb_tools_list_tests} )

    configure_file( list_${_test}.sh.in list_${_test}.sh @ONLY )

    ecbuild_add_test(
        TARGET test_fdb_tools_list_${_test}
        TYPE SCRIPT
        LABELS list
        CONDITION HAVE_FDB_BUILD_TOOLS
        TEST_DEPENDS get_fdb_test_data_list
        COMMAND list_${_test}.sh
        ENVIRONMENT "${test_environment}" )

endforeach( )
