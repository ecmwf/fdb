configure_file( last_access.sh.in last_access.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_last_access
    CONDITION HAVE_GRIB AND HAVE_FDB_BUILD_TOOLS
    TYPE SCRIPT
    TEST_DEPENDS fdb_test_get_data
    COMMAND last_access.sh
    ENVIRONMENT "${test_environment}"
    )
