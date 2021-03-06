configure_file( etc/mars/wind    ${CMAKE_BINARY_DIR}/etc/mars/wind     @ONLY )

configure_file( etc/fdb/schema   ${CMAKE_BINARY_DIR}/etc/fdb/schema    @ONLY )
configure_file( etc/fdb/engines  ${CMAKE_BINARY_DIR}/etc/fdb/engines   @ONLY )

# for file based FDB
configure_file( etc/fdb/dbnames  ${CMAKE_BINARY_DIR}/etc/fdb/dbnames   @ONLY )
configure_file( etc/fdb/roots    ${CMAKE_BINARY_DIR}/etc/fdb/roots     @ONLY )
configure_file( etc/fdb/spaces   ${CMAKE_BINARY_DIR}/etc/fdb/spaces    @ONLY )

file( MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/tests/fdb/root ) # as defined in ${CMAKE_BINARY_DIR}/etc/fdb/roots file

# for pmem based FDB
configure_file( etc/fdb/pools      ${CMAKE_BINARY_DIR}/etc/fdb/pools       @ONLY )
configure_file( etc/fdb/poolgroups ${CMAKE_BINARY_DIR}/etc/fdb/poolgroups  @ONLY )

file( MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/tests/fdb/pool ) # as defined in ${CMAKE_BINARY_DIR}/etc/fdb/pools file

###############################################################################
# fdb unit tests

list( APPEND _test_environment
    FDB_HOME=${CMAKE_BINARY_DIR}
    ECCODES_DEFINITION_PATH=${ECCODES_DEFINITION_PATH}
    ECCODES_SAMPLES_PATH=${ECCODES_SAMPLES_PATH} )

list( APPEND fdb_tests
    test_fdb5_service.cc )

foreach( _tst ${fdb_tests} )

  get_filename_component(_tst ${_tst} NAME_WE)

  ecbuild_add_test(
      TARGET  fdb5_${_tst}
      SOURCES  ${_tst}
      INCLUDES ${ECCODES_INCLUDE_DIRS}
      ENVIRONMENT "${_test_environment}"
      LIBS  fdb5 )

endforeach()

###############################################################################
# fdb tool tests

list( APPEND _test_data
  "rd.vod.grib"
)

ecbuild_get_test_multidata( TARGET fdb5_get_test_data NAMES ${_test_data} )

foreach( _tst ${_test_data} )

    # define tool location using cmake-generator-expressions
    # see https://cmake.org/cmake/help/v3.4/manual/cmake-generator-expressions.7.html

    ecbuild_add_test(
        TARGET  fdb5_archive_${_tst}
        CONDITION HAVE_FDB_BUILD_TOOLS
        COMMAND $<TARGET_FILE:fdb-write>
        TEST_DEPENDS fdb5_get_test_data
        ARGS    ${_tst}
        ENVIRONMENT "${_test_environment}" )

      ecbuild_add_test(
          TARGET  fdb5_extract_${_tst}
          CONDITION HAVE_FDB_BUILD_TOOLS
          COMMAND $<TARGET_FILE:fdb-read>
          TEST_DEPENDS fdb5_archive_${_tst}
          ARGS   --extract ${_tst} ${_tst}.2
          ENVIRONMENT "${_test_environment}" )

endforeach()

#################################################################################
# pmem tests make use of the test environment, so are added at the end

add_subdirectory( pmem )
add_subdirectory( api )
add_subdirectory( tools )
add_subdirectory( type )
