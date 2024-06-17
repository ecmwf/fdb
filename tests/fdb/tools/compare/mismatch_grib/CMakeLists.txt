ecbuild_configure_file( mismatch_grib.sh.in mismatch_grib.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  mismatch_grib.sh
    ENVIRONMENT "${test_environment}" )

