# Run remote API tests, but with the singleConnection option set in the server configs

ecbuild_configure_file( FDB-595.sh.in FDB-595.sh @ONLY )

add_test(NAME test_595
         COMMAND ${CMAKE_CURRENT_BINARY_DIR}/FDB-595.sh)

set_tests_properties(test_595
    PROPERTIES
        ENVIRONMENT "${test_environment}"
        RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
        LABELS remotefdb
        )
