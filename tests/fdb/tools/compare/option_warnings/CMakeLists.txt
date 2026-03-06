ecbuild_configure_file( option_warnings.sh.in option_warnings.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_compare_option_warnings
    TYPE     SCRIPT
    COMMAND  option_warnings.sh
    ENVIRONMENT "${test_environment}" )
