ecbuild_configure_file( FDB-251.sh.in FDB-251.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-251.sh )

