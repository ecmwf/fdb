ecbuild_configure_file( invalid_args.sh.in invalid_args.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_compare_invalid_args
    TYPE     SCRIPT
    COMMAND  invalid_args.sh
    ENVIRONMENT "${test_environment}" )
