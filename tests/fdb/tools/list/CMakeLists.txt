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
        TARGET fdb_test_tools_list_${_test}
        TYPE SCRIPT
        LABELS list
        TEST_DEPENDS fdb_test_get_data
        COMMAND list_${_test}.sh
        ENVIRONMENT "${test_environment}" )

endforeach( )
