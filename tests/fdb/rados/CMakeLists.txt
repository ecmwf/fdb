if (HAVE_RADOSFDB)

  list( APPEND rados_tests
    rados_store
  )

  list( APPEND unit_test_libraries fdb5 )

  foreach( _test ${rados_tests} )

    ecbuild_add_test( TARGET test_fdb5_rados_${_test}
                      SOURCES test_${_test}.cc
                      LIBS "${unit_test_libraries}"
                      INCLUDES "${unit_test_include_dirs}" )

  endforeach()

endif()