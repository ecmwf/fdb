if(metkit_HAVE_ODB)

ecbuild_configure_file( FDB-241.sh.in FDB-241.sh @ONLY ) # needs metkit_HAVE_ODB
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-241.sh )

endif()
