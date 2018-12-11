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
# pointdb tests

ecbuild_add_test( TARGET     mars_server_test_kdtree
                  CONDITION  ENABLE_POINTDB AND OFF
                  INCLUDES
                      ${GRIB_API_INCLUDE_DIRS}
                  SOURCES    test_kdtree.cc
                  LIBS       mars-fdb5 eckit_geometry )

###############################################################################
# fdb unit tests

list( APPEND _test_environment
    FDB_HOME=${CMAKE_BINARY_DIR}
    GRIB_DEFINITION_PATH=${grib_api_BASE_DIR}/share/${grib_handling_pkg}/definitions
    GRIB_SAMPLES_PATH=${grib_api_BASE_DIR}/share/${grib_handling_pkg}/samples )

list( APPEND fdb_tests  test_fdb5_service.cc )

foreach( _tst ${fdb_tests} )

  get_filename_component(_tst ${_tst} NAME_WE)

  ecbuild_add_test(
      TARGET  mars_server_${_tst}
      SOURCES  ${_tst}
      INCLUDES ${GRIB_API_INCLUDE_DIRS}
      ENVIRONMENT "${_test_environment}"
      LIBS  mars-fdb5 )

endforeach()

###############################################################################
# fdb tool tests

list( APPEND _test_data
  "rd.vod.grib"
)

ecbuild_get_test_multidata( TARGET mars_server_fdb5_get_test_data NAMES ${_test_data} )

foreach( _tst ${_test_data} )

    # define tool location using cmake-generator-expressions
    # see https://cmake.org/cmake/help/v3.4/manual/cmake-generator-expressions.7.html

    ecbuild_add_test(
        TARGET  mars_server_fdb5_archive_${_tst}
        COMMAND $<TARGET_FILE:fdb-write>
        TEST_DEPENDS mars_server_fdb5_get_test_data
        ARGS    ${_tst}
        ENVIRONMENT "${_test_environment}" )

      ecbuild_add_test(
          TARGET  mars_server_fdb5_extract_${_tst}
          COMMAND $<TARGET_FILE:fdb-read>
          TEST_DEPENDS mars_server_fdb5_archive_${_tst}
          ARGS   --extract ${_tst} ${_tst}.2
          ENVIRONMENT "${_test_environment}" )

endforeach()

#################################################################################
# pmem tests make use of the test environment, so are added at the end

add_subdirectory( pmem )
add_subdirectory( api )
add_subdirectory( tools )
