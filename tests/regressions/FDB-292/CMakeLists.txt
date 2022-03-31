ecbuild_configure_file( FDB-292.sh.in FDB-292.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-292.sh )
