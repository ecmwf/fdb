ecbuild_configure_file( mars_keys_ignore.sh.in mars_keys_ignore.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_compare_mars_keys_ignore
    TYPE     SCRIPT
    COMMAND  mars_keys_ignore.sh
    ENVIRONMENT "${test_environment}" )
