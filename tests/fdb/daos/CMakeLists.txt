if (HAVE_DUMMY_DAOS)

  list( APPEND dummy_daos_tests
    write_read
  )
  
  foreach( _test ${dummy_daos_tests} )
  
    ecbuild_add_test( TARGET test_fdb5_dummy_daos_${_test}
                      SOURCES test_dummy_daos_${_test}.cc
                      LIBS "${DAOS_LIBRARIES}" ${DAOS_TESTS_LIBRARIES} fdb5
                      INCLUDES "${DAOS_INCLUDE_DIRS}" "${DAOS_TESTS_INCLUDE_DIRS}" 
                      RUN_SERIAL
                      ENVIRONMENT "{test_environment}" )
  
  endforeach()

endif()

if (HAVE_DAOSFDB)

  list( APPEND daos_tests
    daos_handle
    daos_store
    daos_catalogue
  )

  list( APPEND unit_test_libraries "${DAOS_LIBRARIES}" )
  list( APPEND unit_test_include_dirs "${DAOS_INCLUDE_DIRS}" )

  if(HAVE_DAOS_ADMIN)
    list( APPEND unit_test_libraries "${DAOS_TESTS_LIBRARIES}" )
    list( APPEND unit_test_include_dirs "${DAOS_TESTS_INCLUDE_DIRS}" )
  endif()

  list( APPEND unit_test_libraries fdb5 )

  foreach( _test ${daos_tests} )

    ecbuild_add_test( TARGET test_fdb5_daos_${_test}
                      SOURCES test_${_test}.cc
                      LIBS "${unit_test_libraries}"
                      INCLUDES "${unit_test_include_dirs}" 
                      ENVIRONMENT FDB_DAOS_DMG_CONFIG_FILE=${FDB_DAOS_DMG_CONFIG_FILE}
                      ENVIRONMENT FDB_DAOS_TEST_POOL=${FDB_DAOS_TEST_POOL} 
                      ENVIRONMENT "${test_environment}" )

  endforeach()

endif()
