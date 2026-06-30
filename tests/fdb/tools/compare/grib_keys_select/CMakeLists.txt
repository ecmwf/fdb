ecbuild_configure_file( grib_keys_select.sh.in grib_keys_select.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_compare_grib_keys_select
    TYPE     SCRIPT
    COMMAND  grib_keys_select.sh
    ENVIRONMENT "${test_environment}" )
