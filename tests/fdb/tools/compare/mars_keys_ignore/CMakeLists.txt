ecbuild_configure_file( mars_keys_ignore.sh.in mars_keys_ignore.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  mars_keys_ignore.sh
    ENVIRONMENT "${test_environment}" )
