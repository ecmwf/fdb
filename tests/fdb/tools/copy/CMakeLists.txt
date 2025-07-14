if (HAVE_FDB_BUILD_TOOLS)
    ecbuild_configure_file( fdb_copy.sh.in fdb_copy.sh @ONLY )

    ecbuild_add_test(
        TYPE SCRIPT
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB
        COMMAND fdb_copy.sh
        ENVIRONMENT "${test_environment}")
endif()
