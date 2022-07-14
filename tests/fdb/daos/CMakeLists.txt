if (HAVE_DAOSFDB)

  set(DUMMY_DAOS_DATA_ROOT ${CMAKE_CURRENT_BINARY_DIR}/tmp_dir_fdb5_dummy_daos)

  if (HAVE_DUMMY_DAOS)

    list( APPEND dummy_daos_tests
        write_read
    )
    
    foreach( _test ${dummy_daos_tests} )
    
      ecbuild_add_test( TARGET test_fdb5_dummy_daos_${_test}
                        SOURCES test_dummy_daos_${_test}.cc
                        LIBS ${DAOS_LIBRARIES} fdb5
                        INCLUDES "${DAOS_INCLUDE_DIRS}"
                        ENVIRONMENT DUMMY_DAOS_DATA_ROOT=${DUMMY_DAOS_DATA_ROOT} )
    
    endforeach()

    file(MAKE_DIRECTORY ${DUMMY_DAOS_DATA_ROOT})

  endif()

  list( APPEND daos_tests
    daos_handle
  )

  foreach( _test ${daos_tests} )

    ecbuild_add_test( TARGET test_fdb5_daos_${_test}
                      SOURCES test_${_test}.cc
                      LIBS ${DAOS_LIBRARIES} fdb5
                      INCLUDES "${DAOS_INCLUDE_DIRS}"
                      ENVIRONMENT DUMMY_DAOS_DATA_ROOT=${DUMMY_DAOS_DATA_ROOT} )

  endforeach()

endif()
