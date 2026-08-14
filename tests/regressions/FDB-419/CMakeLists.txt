
set(SCHEMA_PATH ${CMAKE_CURRENT_SOURCE_DIR}/schema)
set(SPACE_ROOT_PATH ${CMAKE_CURRENT_BINARY_DIR})
ecbuild_configure_file(
    fdb_client_config.yaml.in
    fdb_client_config.yaml
    @ONLY
)
ecbuild_configure_file(
    fdb_server_config.yaml.in
    fdb_server_config.yaml
    @ONLY
)
ecbuild_configure_file( tsan_suppressions.txt.in tsan_suppressions.txt @ONLY )

ecbuild_add_test(
    TARGET FDB-419
    SOURCES fdb_419_regression_test.cc
    LIBS fdb5 eckit metkit
    ENVIRONMENT FDB_SERVER_EXECUTABLE=$<TARGET_FILE:fdb-server>
    ENVIRONMENT "${test_environment}"
    ENVIRONMENT "TSAN_OPTIONS=suppressions=${CMAKE_CURRENT_BINARY_DIR}/tsan_suppressions.txt"
    TEST_PROPERTIES
    RESOURCE_LOCK fdb_remote_fdb419
    LABELS remotefdb
)
