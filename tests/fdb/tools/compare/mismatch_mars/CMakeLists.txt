ecbuild_configure_file( mismatch_mars.sh.in mismatch_mars.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  mismatch_mars.sh
    ENVIRONMENT "${test_environment}" )

