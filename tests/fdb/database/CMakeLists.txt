
list( APPEND _test_environment
    FDB_HOME=${PROJECT_BINARY_DIR} )

ecbuild_add_test( TARGET test_fdb5_database_indexaxis
    SOURCES test_indexaxis.cc
    INCLUDES ${PMEM_INCLUDE_DIRS}
    LIBS fdb5
    ENVIRONMENT "${_test_environment}")

ecbuild_add_test( TARGET test_fdb5_fieldlocation
    SOURCES test_fieldlocation.cc
    LIBS fdb5
    ENVIRONMENT "${_test_environment}")
