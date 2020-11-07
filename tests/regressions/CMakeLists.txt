if (HAVE_FDB_BUILD_TOOLS) # test scripts use the fdb tools

  ecbuild_get_test_multidata(TARGET download_fdb5_regression_tests_files NAMES sh.300.grib )

  list( APPEND regressions FDB-238 FDB-240 )

  foreach( r ${regressions} )

    ecbuild_configure_file( ${r}.sh.in ${r}.sh @ONLY )

    ecbuild_add_test(
      TYPE     SCRIPT
      COMMAND  ${r}.sh )

  endforeach()

endif()

