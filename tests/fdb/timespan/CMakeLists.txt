ecbuild_configure_file( timespan.sh.in timespan.sh @ONLY )
ecbuild_add_test(
    TARGET   fdb_test_timespan
    CONDITION HAVE_GRIB
    LABELS   mtg2
    TYPE     SCRIPT
    COMMAND  timespan.sh
    ENVIRONMENT "${test_environment}" )
