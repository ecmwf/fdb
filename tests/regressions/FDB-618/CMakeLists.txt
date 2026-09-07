ecbuild_configure_file( FDB-618.sh.in FDB-618.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-618.sh
    ENVIRONMENT "${test_environment}" )
