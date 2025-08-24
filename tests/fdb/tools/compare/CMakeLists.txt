# # get test data for the list tests
# ecbuild_get_test_multidata( TARGET get_fdb_test_data_list LABELS list NAMES "od.oper.grib" )

# list of tests
list( APPEND fdb_tools_compare_tests
    all_identical
    diff_exp
    mismatch_mars

)
configure_file( setup_ref_fdb.sh.in setup_ref_fdb.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_tools_compare_setup_ref_fdb
    TYPE SCRIPT
    LABELS compare
    CONDITION HAVE_FDB_BUILD_TOOLS
    COMMAND setup_ref_fdb.sh
    ENVIRONMENT "${test_environment}" )


# add the tests
foreach( _test ${fdb_tools_compare_tests} )

    configure_file( compare_${_test}.sh.in compare_${_test}.sh @ONLY )

    ecbuild_add_test(
        TARGET test_fdb_tools_compare_${_test}
        TYPE SCRIPT
        LABELS compare
        CONDITION HAVE_FDB_BUILD_TOOLS
        TEST_DEPENDS setup_ref_fdb
        COMMAND compare_${_test}.sh
        ENVIRONMENT "${test_environment}" )

endforeach( )
