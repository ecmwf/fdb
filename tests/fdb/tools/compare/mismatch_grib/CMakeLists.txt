ecbuild_configure_file( mismatch_grib.sh.in mismatch_grib.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_compare_mismatch_grib
    TYPE     SCRIPT
    COMMAND  mismatch_grib.sh
    ENVIRONMENT "${test_environment}" )

