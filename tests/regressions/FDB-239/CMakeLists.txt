ecbuild_configure_file( FDB-239.sh.in FDB-239.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-239.sh
    ENVIRONMENT "${test_environment}" )

