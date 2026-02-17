### test data files
list( APPEND fdb_api_test_data_files
x138-300.grib
x138-400.grib
y138-400.grib
)

set( _test_environment ${test_environment})

list( APPEND _test_environment
    FDB_HOME=${PROJECT_BINARY_DIR} )
    
ecbuild_get_test_multidata( TARGET get_fdb_api_test_data
                            NAMES ${fdb_api_test_data_files}
                            NOCHECK )

if (HAVE_DAOSFDB)

  list( APPEND unit_test_libraries "${DAOS_LIBRARIES}" )
  list( APPEND unit_test_include_dirs "${DAOS_INCLUDE_DIRS}" )

  if(HAVE_DAOS_ADMIN)
    list( APPEND unit_test_libraries "${DAOS_TESTS_LIBRARIES}" )
    list( APPEND unit_test_include_dirs "${DAOS_TESTS_INCLUDE_DIRS}" )
  endif()

endif()

list( APPEND unit_test_libraries fdb5 )

list( APPEND api_tests
    config
    select
    dist
    fdb_c
    callback
    auxiliary
    select_exclude
    wipe
)

if (HAVE_DAOSFDB)

  list( APPEND api_tests
      wipe_daos
  )

endif()

foreach( _test ${api_tests} )

    ecbuild_add_test( TARGET test_fdb5_api_${_test}
                      SOURCES test_${_test}.cc
                      TEST_DEPENDS get_fdb_api_test_data
                      LIBS "${unit_test_libraries}"
                      INCLUDES "${unit_test_include_dirs}"
                      ENVIRONMENT FDB_DAOS_DMG_CONFIG_FILE=${FDB_DAOS_DMG_CONFIG_FILE}
                      ENVIRONMENT FDB_DAOS_TEST_POOL=${FDB_DAOS_TEST_POOL}
                      ENVIRONMENT "${_test_environment}" )

endforeach()
