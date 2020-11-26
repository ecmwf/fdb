ecbuild_configure_file( FDB-238.sh.in FDB-238.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-238.sh )

