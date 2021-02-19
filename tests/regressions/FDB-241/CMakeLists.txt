ecbuild_configure_file( FDB-241.sh.in FDB-241.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-241.sh )

