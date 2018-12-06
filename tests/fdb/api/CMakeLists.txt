
list( APPEND api_tests
    config
    select
)

foreach( _test ${api_tests} )

    ecbuild_add_test( TARGET test_fdb5_api_${_test}
                      SOURCES test_${_test}.cc
                      LIBS mars-fdb5)

endforeach()
