ecbuild_configure_file( FDB-267.sh.in FDB-267.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-267.sh )

