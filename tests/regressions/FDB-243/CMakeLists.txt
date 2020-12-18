ecbuild_configure_file( FDB-243.sh.in FDB-243.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-243.sh )

