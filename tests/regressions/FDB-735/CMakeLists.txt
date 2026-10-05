ecbuild_configure_file( FDB-735.sh.in FDB-735.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-735.sh
    ENVIRONMENT "${test_environment}" )
