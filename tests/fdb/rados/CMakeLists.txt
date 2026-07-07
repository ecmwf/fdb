if (HAVE_RADOSFDB)

  list( APPEND rados_tests
    rados_store
    rados_catalogue
  )

  list( APPEND unit_test_libraries fdb5 )

  # The Rados unit tests need a pool to run against (with RADOS_ADMIN=OFF the pool
  # must already exist, e.g. one created in the Ceph service). The pool name can be
  # provided at configure time via -DFDB_RADOS_TEST_POOL=<pool>, or inherited from
  # the FDB_RADOS_TEST_POOL environment variable (e.g. exported in the dev container
  # that talks to the Ceph service).
  if( NOT FDB_RADOS_TEST_POOL AND DEFINED ENV{FDB_RADOS_TEST_POOL} )
    set( FDB_RADOS_TEST_POOL "$ENV{FDB_RADOS_TEST_POOL}" )
  endif()

  # Only inject the variable into the test environment when we have a value:
  # passing an empty "FDB_RADOS_TEST_POOL=" would clobber any value already present
  # in the runtime environment when the test executes.
  unset( _rados_test_environment )
  if( FDB_RADOS_TEST_POOL )
    set( _rados_test_environment ENVIRONMENT FDB_RADOS_TEST_POOL=${FDB_RADOS_TEST_POOL} )
  endif()

  foreach( _test ${rados_tests} )

    ecbuild_add_test( TARGET  fdb_test_${_test}
                      SOURCES test_${_test}.cc
                      LABELS  rados
                      LIBS "${unit_test_libraries}"
                      INCLUDES "${unit_test_include_dirs}"
                      ${_rados_test_environment} )

  endforeach()

endif()
