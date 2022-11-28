set(DUMMY_DAOS_DATA_ROOT ${CMAKE_CURRENT_BINARY_DIR}/tmp_dir_fdb5_dummy_daos)
set(FDB_ROOT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/fdb_root)

if (HAVE_DUMMY_DAOS)

  file(REMOVE_RECURSE ${DUMMY_DAOS_DATA_ROOT})
  file(MAKE_DIRECTORY ${DUMMY_DAOS_DATA_ROOT})

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

endif()

if (HAVE_DAOSFDB)

  file(REMOVE_RECURSE ${FDB_ROOT_DIRECTORY})
  file(MAKE_DIRECTORY ${FDB_ROOT_DIRECTORY})

  list( APPEND daos_tests
    daos_handle daos_store
  )

  foreach( _test ${daos_tests} )

    ecbuild_add_test( TARGET test_fdb5_daos_${_test}
                      SOURCES test_${_test}.cc
                      LIBS ${DAOS_LIBRARIES} fdb5
                      INCLUDES "${DAOS_INCLUDE_DIRS}"
                      ENVIRONMENT DUMMY_DAOS_DATA_ROOT=${DUMMY_DAOS_DATA_ROOT}
                      ENVIRONMENT FDB_ROOT_DIRECTORY=${FDB_ROOT_DIRECTORY} )

  endforeach()

endif()
