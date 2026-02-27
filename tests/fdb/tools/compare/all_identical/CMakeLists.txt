ecbuild_configure_file( all_identical.sh.in all_identical.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_compare_all_identical
    TYPE     SCRIPT
    COMMAND  all_identical.sh
    ENVIRONMENT "${test_environment}" )

