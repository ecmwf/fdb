ecbuild_configure_file( FDB-240.sh.in FDB-240.sh @ONLY )
ecbuild_get_test_multidata(TARGET fdb_test_get_data_regression NAMES sh.300.grib NOCHECK )

ecbuild_add_test(
    TYPE     SCRIPT
    TEST_DEPENDS fdb_test_get_data_regression
    COMMAND  FDB-240.sh
    ENVIRONMENT "${test_environment}" )

