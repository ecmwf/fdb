ecbuild_configure_file( FDB-258.sh.in FDB-258.sh @ONLY )

ecbuild_add_test(
    TYPE      SCRIPT
    CONDITION HAVE_EXTRA_TESTS
    COMMAND   FDB-258.sh
    ENVIRONMENT "${test_environment}" )
