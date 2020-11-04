list( APPEND regressions FDB-238 )

foreach( r ${regressions} )

  ecbuild_configure_file( ${r}.sh.in ${r}.sh @ONLY )

  ecbuild_add_test(
    TYPE     SCRIPT
    CONDITION HAVE_FDB_BUILD_TOOLS
    COMMAND  ${r}.sh )

endforeach()

