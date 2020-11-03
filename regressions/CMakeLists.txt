list( APPEND regressions FDB-238 )

foreach( r ${regressions} )

  ecbuild_configure_file( ${r}.sh.in ${r}.sh @ONLY )

  ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  ${r}.sh )

endforeach()

