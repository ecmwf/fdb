
list( APPEND api_tests
    config
    select
    dist
    fdb_c
)

foreach( _test ${api_tests} )

    ecbuild_add_test( TARGET test_fdb5_api_${_test}
                      SOURCES test_${_test}.cc
                      LIBS fdb5)

endforeach()
