ecbuild_configure_file( FDB-332.sh.in FDB-332.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-332.sh
    ENVIRONMENT "${test_environment}" )
