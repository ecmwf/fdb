# Run remote API tests, but with the singleConnection option set in the server configs

ecbuild_configure_file( FDB-595.sh.in FDB-595.sh @ONLY )

ecbuild_add_test(
    TARGET         FDB-595
    TYPE           SCRIPT
    COMMAND        ${CMAKE_CURRENT_BINARY_DIR}/FDB-595.sh
    TEST_DEPENDS   fdb_test_remote_cleanup
    ENVIRONMENT    "${test_environment}"
    TEST_PROPERTIES
    RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
    LABELS remotefdb
)
