ecbuild_configure_file( invalid_args.sh.in invalid_args.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  invalid_args.sh
    ENVIRONMENT "${test_environment}" )
