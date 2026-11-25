set( _test_environment ${test_environment})

list( APPEND _test_environment
    FDB_HOME=${PROJECT_BINARY_DIR} )

ecbuild_add_test( TARGET fdb_test_database_indexaxis
    SOURCES test_indexaxis.cc
    LIBS fdb5
    ENVIRONMENT "${_test_environment}")

# Inherit the harness settings at test time: the mount may not be visible where CMake runs.
ecbuild_add_test( TARGET fdb_test_database_nfs
    CONDITION HAVE_TOCFDB
    SOURCES test_nfs.cc
    LIBS fdb5
    LABELS nfs
    ENVIRONMENT "${_test_environment}")

ecbuild_add_test( TARGET fdb_test_database_orderedparallelfor
    SOURCES test_orderedparallelfor.cc
    LIBS fdb5
    ENVIRONMENT "${_test_environment}")
