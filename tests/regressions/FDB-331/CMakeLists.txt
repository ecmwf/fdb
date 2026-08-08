ecbuild_configure_file( FDB-331.sh.in FDB-331.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-331.sh
    ENVIRONMENT "${test_environment}" )
