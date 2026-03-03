ecbuild_configure_file( FDB-541.sh.in FDB-541.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-541.sh
    ENVIRONMENT "${test_environment}" )
