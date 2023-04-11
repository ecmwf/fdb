if (HAVE_DUMMY_DAOS AND HAVE_DAOS_ADMIN)

  list( APPEND dummy_daos_tests
    write_read
  )
  
  foreach( _test ${dummy_daos_tests} )
  
    ecbuild_add_test( TARGET test_fdb5_dummy_daos_${_test}
                      SOURCES test_dummy_daos_${_test}.cc
                      LIBS ${DAOS_LIBRARIES} ${DAOS_TESTS_LIBRARIES} fdb5
                      INCLUDES "${DAOS_INCLUDE_DIRS}" "${DAOS_TESTS_INCLUDE_DIRS}" )
  
  endforeach()

endif()

if (HAVE_DAOSFDB AND HAVE_DAOS_ADMIN)

  list( APPEND daos_tests
    daos_handle daos_store daos_catalogue
  )

  foreach( _test ${daos_tests} )

    ecbuild_add_test( TARGET test_fdb5_daos_${_test}
                      SOURCES test_${_test}.cc
                      LIBS ${DAOS_LIBRARIES} ${DAOS_TESTS_LIBRARIES} fdb5
                      INCLUDES "${DAOS_INCLUDE_DIRS}" "${DAOS_TESTS_INCLUDE_DIRS}" )

  endforeach()

endif()
