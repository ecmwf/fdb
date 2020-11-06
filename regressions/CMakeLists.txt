if (HAVE_FDB_BUILD_TOOLS) # test scripts use the fdb tools

  list( APPEND regressions FDB-238 )

  foreach( r ${regressions} )

    ecbuild_configure_file( ${r}.sh.in ${r}.sh @ONLY )

    ecbuild_add_test(
      TYPE     SCRIPT
      COMMAND  ${r}.sh )

  endforeach()

  list( APPEND regressionsBin FDB-239 )

  foreach( r ${regressionsBin} )

    ecbuild_configure_file( ${r}.sh.in ${r}.sh @ONLY )

    ecbuild_add_executable(
      NOINSTALL
      TARGET   ${r}.x
      SOURCES  ${r}.cc
      LIBS     fdb5 )

    ecbuild_add_test(
      TYPE     SCRIPT
      COMMAND  ${r}.sh )

  endforeach()

endif()

