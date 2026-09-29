ecbuild_configure_file( move_auxiliary.sh.in fdb_move_auxiliary.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_move
    TYPE SCRIPT
    CONDITION HAVE_GRIB
    COMMAND fdb_move_auxiliary.sh
    ENVIRONMENT "${test_environment}")
