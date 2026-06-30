ecbuild_configure_file( FDB-535.sh.in FDB-535.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-535.sh
    ENVIRONMENT "${test_environment}" )

