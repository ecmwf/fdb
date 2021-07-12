ecbuild_configure_file( FDB-275.sh.in FDB-275.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-275.sh )
