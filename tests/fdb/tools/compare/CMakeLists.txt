# # get test data for the list tests
# ecbuild_get_test_multidata( TARGET get_fdb_test_data_list LABELS list NAMES "od.oper.grib" )

# list of tests
list( APPEND fdb_tools_list_tests
    all_identical

)

# add the tests
foreach( _test ${fdb_tools_list_tests} )

    configure_file( compare_${_test}.sh.in compare_${_test}.sh @ONLY )

    ecbuild_add_test(
        TARGET test_fdb_tools_compare_${_test}
        TYPE SCRIPT
        LABELS list
        CONDITION HAVE_FDB_BUILD_TOOLS
        # TEST_DEPENDS get_fdb_test_data_list
        COMMAND compare_${_test}.sh
        ENVIRONMENT "${test_environment}" )

endforeach( )
