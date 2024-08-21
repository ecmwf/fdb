### test data files

list( APPEND _test_environment
    FDB_HOME=${PROJECT_BINARY_DIR} )

if(ECCODES_DEFINITION_PATH AND (EXISTS ${ECCODES_DEFINITION_PATH}))
    list( APPEND _test_environment 
        ECCODES_DEFINITION_PATH=${ECCODES_DEFINITION_PATH})
endif()

if(ECCODES_SAMPLES_PATH AND (EXISTS ${ECCODES_SAMPLES_PATH}))
    list( APPEND _test_environment 
    ECCODES_SAMPLES_PATH=${ECCODES_SAMPLES_PATH})
endif()


list( APPEND plugin_tests
    gribjump
)

foreach( _test ${plugin_tests} )

    ecbuild_add_test( TARGET test_fdb5_plugin_${_test}
                      SOURCES test_${_test}_plugin.cc
                      TEST_DEPENDS get_fdb_api_test_data
                      LIBS fdb5
                      ENVIRONMENT "${_test_environment}" )

endforeach()
