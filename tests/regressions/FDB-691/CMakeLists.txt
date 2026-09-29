ecbuild_configure_file( FDB-691.sh.in FDB-691.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-691.sh
    ENVIRONMENT "${test_environment}" )
