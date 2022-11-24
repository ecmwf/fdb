### test data files
list( APPEND fdb_api_test_data_files
x138-300.grib
x138-400.grib
y138-400.grib
)

list( APPEND _test_environment
    FDB_HOME=${PROJECT_BINARY_DIR} )
    # ECCODES_DEFINITION_PATH=${ECCODES_DEFINITION_PATH}
    # ECCODES_SAMPLES_PATH=${ECCODES_SAMPLES_PATH} )

ecbuild_get_test_multidata( TARGET get_fdb_api_test_data
                            NAMES ${fdb_api_test_data_files}
                            NOCHECK )

list( APPEND api_tests
    config
    select
    dist
    fdb_c
)

foreach( _test ${api_tests} )

    ecbuild_add_test( TARGET test_fdb5_api_${_test}
                      SOURCES test_${_test}.cc
                      TEST_DEPENDS get_fdb_api_test_data
                      LIBS fdb5
                      ENVIRONMENT "${_test_environment}" )

endforeach()
