set( _test_environment ${test_environment})

list( APPEND _test_environment
    FDB_HOME=${PROJECT_BINARY_DIR} )

file(COPY data DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

ecbuild_add_test( TARGET fdb_test_schema_parsing
    SOURCES 
        test_schema_parsing.cc
    LIBS 
        fdb5
    ENVIRONMENT "${_test_environment}")
