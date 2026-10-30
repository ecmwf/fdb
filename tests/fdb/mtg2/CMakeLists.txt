ecbuild_configure_file( alternativeParams.sh.in alternativeParams.sh @ONLY )
ecbuild_add_test(
    TARGET fdb_test_alternative_params
    TYPE     SCRIPT
    COMMAND  alternativeParams.sh
    ENVIRONMENT "${test_environment}" )
