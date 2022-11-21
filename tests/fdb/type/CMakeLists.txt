
list( APPEND _test_environment
    FDB_HOME=${CMAKE_BINARY_DIR}/fdb5
)

list( APPEND type_tests
    toKey
)

foreach( _test ${type_tests} )

    ecbuild_add_test( TARGET test_fdb5_type_${_test}
                      SOURCES test_${_test}.cc
                      ENVIRONMENT ${_test_environment}
                      LIBS fdb5)

endforeach()
