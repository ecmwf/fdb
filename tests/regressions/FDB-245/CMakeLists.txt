ecbuild_configure_file( FDB-245.sh.in FDB-245.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-245.sh
    ENVIRONMENT "${test_environment}" )

