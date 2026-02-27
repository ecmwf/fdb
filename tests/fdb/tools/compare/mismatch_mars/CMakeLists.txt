ecbuild_configure_file( mismatch_mars.sh.in mismatch_mars.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_compare_mismatch_mars
    TYPE     SCRIPT
    COMMAND  mismatch_mars.sh
    ENVIRONMENT "${test_environment}" )

