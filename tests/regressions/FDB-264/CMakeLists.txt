ecbuild_configure_file( FDB-264.sh.in FDB-264.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-264.sh
    ENVIRONMENT "${test_environment}" )

