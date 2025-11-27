# Run remote API tests, but with the singleConnection option set in the server configs

ecbuild_configure_file( FDB-595.sh.in FDB-595.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-595.sh
    ENVIRONMENT "${test_environment}" )