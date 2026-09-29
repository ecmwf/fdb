ecbuild_configure_file( FDB-307.sh.in FDB-307.sh @ONLY )
ecbuild_configure_file( tsan_suppressions.txt.in tsan_suppressions.txt @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-307.sh
    ENVIRONMENT "${test_environment}" )
