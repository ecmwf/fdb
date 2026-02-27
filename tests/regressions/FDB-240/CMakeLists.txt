ecbuild_configure_file( FDB-240.sh.in FDB-240.sh @ONLY )
ecbuild_get_test_multidata(TARGET download_fdb5_regression_tests_files NAMES sh.300.grib NOCHECK )

ecbuild_add_test(
    TYPE     SCRIPT
    TEST_DEPENDS download_fdb5_regression_tests_files
    COMMAND  FDB-240.sh
    ENVIRONMENT "${test_environment}" )

