list( APPEND type_tests
    toKey
)

foreach( _test ${type_tests} )

    ecbuild_add_test( TARGET test_fdb5_type_${_test}
                      SOURCES test_${_test}.cc
                      LIBS fdb5)

endforeach()
