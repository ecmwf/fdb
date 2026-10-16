configure_file( touch.sh.in touch.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_touch
    CONDITION HAVE_GRIB AND HAVE_FDB_BUILD_TOOLS
    TYPE SCRIPT
    TEST_DEPENDS fdb_test_get_data
    COMMAND touch.sh
    ENVIRONMENT "${test_environment}" )
