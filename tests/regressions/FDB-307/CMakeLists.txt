ecbuild_configure_file( FDB-307.sh.in FDB-307.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-307.sh )
