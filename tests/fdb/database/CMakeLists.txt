set( _test_environment ${test_environment})

list( APPEND _test_environment
    FDB_HOME=${PROJECT_BINARY_DIR} )

ecbuild_add_test( TARGET fdb_test_database_indexaxis
    SOURCES test_indexaxis.cc
    LIBS fdb5
    ENVIRONMENT "${_test_environment}")
