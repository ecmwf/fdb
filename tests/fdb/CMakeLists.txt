configure_file( etc/mars/wind       ${PROJECT_BINARY_DIR}/etc/mars/wind       @ONLY )

configure_file( etc/fdb/config.yaml ${PROJECT_BINARY_DIR}/etc/fdb/config.yaml @ONLY )
configure_file( etc/fdb/schema      ${PROJECT_BINARY_DIR}/etc/fdb/schema      @ONLY )
#configure_file( etc/fdb/engines  ${CMAKE_BINARY_DIR}/etc/fdb/engines   @ONLY )

file( MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/tests/fdb/root ) # as defined in ${CMAKE_BINARY_DIR}/etc/fdb/roots file
file( MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/tests/fdb/root2 ) # as defined in ${CMAKE_BINARY_DIR}/etc/fdb/roots file

file( MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/tests/fdb/pool ) # as defined in ${CMAKE_BINARY_DIR}/etc/fdb/pools file

###############################################################################
# fdb unit tests

set( _test_environment ${test_environment})

list( APPEND _test_environment
    FDB_HOME=${PROJECT_BINARY_DIR} )

list( APPEND fdb_tests
    test_fdb5_service.cc )

foreach( _tst ${fdb_tests} )

  get_filename_component(_test_name ${_tst} NAME_WE)

  ecbuild_add_test(
      TARGET  fdb5_${_test_name}
      SOURCES  ${_tst}
      INCLUDES ${ECCODES_INCLUDE_DIRS}
      ENVIRONMENT "${_test_environment}"
      LIBS  fdb5 )

endforeach()

###############################################################################
# fdb tool tests

list( APPEND _test_data
  "rd.vod.grib"
  "od.oper.grib"
)

ecbuild_get_test_multidata( TARGET fdb5_get_test_data NAMES ${_test_data} NOCHECK )

foreach( _tst ${_test_data} )

    # define tool location using cmake-generator-expressions
    # see https://cmake.org/cmake/help/v3.4/manual/cmake-generator-expressions.7.html

    ecbuild_add_test(
        TARGET  fdb5_archive_${_tst}
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB
        COMMAND $<TARGET_FILE:fdb-write>
        TEST_DEPENDS fdb5_get_test_data
        ARGS    ${_tst}
        ENVIRONMENT "${_test_environment}" )

      ecbuild_add_test(
          TARGET  fdb5_extract_${_tst}
          CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB
          COMMAND $<TARGET_FILE:fdb-read>
          TEST_DEPENDS fdb5_archive_${_tst}
          ARGS   --extract ${_tst} ${_tst}.2
          ENVIRONMENT "${_test_environment}" )

endforeach()

add_subdirectory( api )
add_subdirectory( database )
add_subdirectory( tools )
add_subdirectory( type )
add_subdirectory( daos )

if (HAVE_FDB_REMOTE)
    add_subdirectory( remote )
endif()