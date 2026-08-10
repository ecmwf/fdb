ecbuild_configure_file( FDB-714.sh.in FDB-714.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-714.sh
    ENVIRONMENT "${test_environment}" )
