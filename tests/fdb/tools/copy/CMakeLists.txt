if (HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB)
    ecbuild_configure_file( fdb_copy.sh.in fdb_copy.sh @ONLY )

    ecbuild_add_test(
        TYPE SCRIPT
        COMMAND fdb_copy.sh
        ENVIRONMENT "${test_environment}")
endif()
