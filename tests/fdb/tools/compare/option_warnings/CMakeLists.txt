ecbuild_configure_file( option_warnings.sh.in option_warnings.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  option_warnings.sh
    ENVIRONMENT "${test_environment}" )
