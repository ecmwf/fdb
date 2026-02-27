ecbuild_configure_file( diff_exp.sh.in diff_exp.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_compare_diff_exp
    TYPE     SCRIPT
    COMMAND  diff_exp.sh
    ENVIRONMENT "${test_environment}" )

