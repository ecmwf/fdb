ecbuild_configure_file( FDB-276.sh.in FDB-276.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-276.sh
    ENVIRONMENT "${test_environment}" )
