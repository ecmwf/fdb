ecbuild_configure_file( mtg2.sh.in mtg2.sh @ONLY )
ecbuild_add_test(
    TARGET   fdb_test_mtg2
    CONDITION HAVE_GRIB
    LABELS   mtg2
    TYPE     SCRIPT
    COMMAND  mtg2.sh
    ENVIRONMENT "${test_environment}" )
