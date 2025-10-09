if (HAVE_FDB_BUILD_TOOLS)
    ecbuild_configure_file( move_auxiliary.sh.in fdb_move_auxiliary.sh @ONLY )

    ecbuild_add_test(
        TYPE SCRIPT
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB
        COMMAND fdb_move_auxiliary.sh
        ENVIRONMENT "${test_environment}")
endif()

