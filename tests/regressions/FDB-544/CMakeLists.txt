ecbuild_configure_file( FDB-544.sh.in FDB-544.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-544.sh
    ENVIRONMENT "${test_environment}" )
