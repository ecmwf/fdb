if (HAVE_S3FDB)

  list( APPEND s3_tests
    s3_store
  )

  list( APPEND unit_test_libraries fdb5 )

  foreach( _test ${s3_tests} )

    ecbuild_add_test( TARGET test_fdb5_s3_${_test}
                      SOURCES test_${_test}.cc
                      LIBS "${unit_test_libraries}"
                      INCLUDES "${unit_test_include_dirs}" )

  endforeach()

endif()