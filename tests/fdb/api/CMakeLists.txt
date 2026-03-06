### test data files
list( APPEND fdb_api_test_data_files
x138-300.grib
x138-400.grib
y138-400.grib
)

set( _test_environment ${test_environment})

list( APPEND _test_environment
    FDB_HOME=${PROJECT_BINARY_DIR} )
    
ecbuild_get_test_multidata( TARGET fdb_test_get_api_data
                            NAMES ${fdb_api_test_data_files}
                            NOCHECK )

list( APPEND api_tests
    config
    select
    dist
    fdb_c
    callback
    auxiliary
    select_exclude
)

foreach( _test ${api_tests} )

    ecbuild_add_test( TARGET fdb_test_api_${_test}
                      SOURCES test_${_test}.cc
                      TEST_DEPENDS fdb_test_get_api_data
                      LIBS fdb5
                      ENVIRONMENT "${_test_environment}" )

endforeach()
