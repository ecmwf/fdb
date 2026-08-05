ecbuild_configure_file( FDB-709.sh.in FDB-709.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-709.sh
    ENVIRONMENT "${test_environment}" )
