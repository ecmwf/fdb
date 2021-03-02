ecbuild_configure_file( remote.sh.in remote.sh @ONLY )

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  remote.sh )

