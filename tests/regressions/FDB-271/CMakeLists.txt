ecbuild_configure_file( FDB-271.sh.in FDB-271.sh @ONLY )

if( CMAKE_SYSTEM_NAME MATCHES "Linux" )
    ecbuild_add_test(
        TYPE     SCRIPT
        COMMAND  FDB-271.sh )
endif()
