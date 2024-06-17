ecbuild_configure_file( diff_exp.sh.in diff_exp.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  diff_exp.sh
    ENVIRONMENT "${test_environment}" )

