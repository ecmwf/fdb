ecbuild_configure_file( FDB-282.sh.in FDB-282.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-282.sh
    ENVIRONMENT "${test_environment}" )
