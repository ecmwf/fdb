ecbuild_configure_file( FDB-266.sh.in FDB-266.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-266.sh )

