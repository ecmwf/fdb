set( _test_environment ${test_environment})

list( APPEND _test_environment
    FDB_HOME=${PROJECT_BINARY_DIR} )

ecbuild_add_test( TARGET fdb_test_database_indexaxis
    SOURCES test_indexaxis.cc
    LIBS fdb5
    ENVIRONMENT "${_test_environment}")

set( _nfs_test_environment ${_test_environment} )
# Forward the NFS harness location so the NFS-backed case runs when an export is mounted
# (e.g. a docker nfs-server). Only set when non-empty at configure time, so a value provided
# at test time via the inherited environment is not overridden with an empty string. Unset
# either way -> the case self-skips.
if( NOT "$ENV{FDB_TEST_NFS_MOUNT}" STREQUAL "" )
    list( APPEND _nfs_test_environment "FDB_TEST_NFS_MOUNT=$ENV{FDB_TEST_NFS_MOUNT}" )
endif()

ecbuild_add_test( TARGET fdb_test_database_nfs
    SOURCES test_nfs.cc
    LIBS fdb5
    LABELS nfs
    ENVIRONMENT "${_nfs_test_environment}")
