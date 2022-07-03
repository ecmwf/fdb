if (HAVE_DUMMY_DAOS)

  list( APPEND dummy_daos_tests
      write_read
  )
  
  foreach( _test ${dummy_daos_tests} )
  
      ecbuild_add_test( TARGET test_fdb5_dummy_daos_${_test}
                        SOURCES test_dummy_daos_${_test}.cc
                        LIBS daos fdb5
                        ENVIRONMENT DUMMY_DAOS_DATA_ROOT=${CMAKE_CURRENT_BINARY_DIR}/tmp_dir_fdb5_dummy_daos )
  
  endforeach()

  file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tmp_dir_fdb5_dummy_daos)

endif()
