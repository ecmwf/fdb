ecbuild_configure_file( FDB-260.sh.in FDB-260.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-260.sh )

