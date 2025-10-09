if (HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB)
    ecbuild_configure_file( fdb_reindex.sh.in fdb_reindex.sh @ONLY )

    ecbuild_add_test(
        TYPE SCRIPT
        COMMAND fdb_reindex.sh
        ENVIRONMENT "${test_environment}")
endif()
