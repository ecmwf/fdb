ecbuild_configure_file( all_identical.sh.in all_identical.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  all_identical.sh
    ENVIRONMENT "${test_environment}" )

