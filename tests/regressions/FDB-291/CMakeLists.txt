ecbuild_configure_file( FDB-291.sh.in FDB-291.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-291.sh )
