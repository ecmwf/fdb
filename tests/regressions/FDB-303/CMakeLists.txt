ecbuild_configure_file( FDB-303.sh.in FDB-303.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-303.sh )
