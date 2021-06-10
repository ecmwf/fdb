ecbuild_configure_file( FDB-268_backComp.sh.in FDB-268_backComp.sh @ONLY )
ecbuild_configure_file( FDB-268.sh.in FDB-268.sh @ONLY )

if( CMAKE_SYSTEM_NAME MATCHES "Linux" )
    ecbuild_add_test(
        TYPE     SCRIPT
        COMMAND  FDB-268_backComp.sh )
endif()

ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-268.sh )
