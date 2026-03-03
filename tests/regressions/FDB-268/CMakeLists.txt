ecbuild_configure_file( FDB-268.sh.in FDB-268.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-268.sh
    ENVIRONMENT "${test_environment}" )
