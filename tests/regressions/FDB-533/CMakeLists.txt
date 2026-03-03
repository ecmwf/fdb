ecbuild_configure_file( FDB-533.sh.in FDB-533.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-533.sh
    ENVIRONMENT "${test_environment}" )

