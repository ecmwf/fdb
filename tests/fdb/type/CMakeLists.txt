
list( APPEND type_tests
    toKey
)

list( APPEND _test_environment
    FDB_HOME=${PROJECT_BINARY_DIR} )
    # ECCODES_DEFINITION_PATH=${ECCODES_DEFINITION_PATH}
    # ECCODES_SAMPLES_PATH=${ECCODES_SAMPLES_PATH} )

foreach( _test ${type_tests} )

    ecbuild_add_test( TARGET test_fdb5_type_${_test}
                      SOURCES test_${_test}.cc
                      LIBS fdb5
                      ENVIRONMENT "${_test_environment}" )

endforeach()
