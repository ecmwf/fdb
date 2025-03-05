if (HAVE_FDB_BUILD_TOOLS)
    ecbuild_configure_file( fdb_reindex.sh.in fdb_reindex.sh @ONLY )

    ecbuild_add_test(
        TYPE SCRIPT
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB
        COMMAND fdb_reindex.sh)
endif()
