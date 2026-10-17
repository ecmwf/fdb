ecbuild_configure_file( alternativeParams.sh.in alternativeParams.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  alternativeParams.sh
    ENVIRONMENT "${test_environment}" )
