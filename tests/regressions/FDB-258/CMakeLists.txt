ecbuild_configure_file( FDB-258.sh.in FDB-258.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-258.sh )

