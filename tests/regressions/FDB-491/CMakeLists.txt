# Run remote API tests, but with the singleConnection option set in the server configs

if (NOT DEFINED FDB_TEST_SERVER_SCRIPT)
    message(FATAL_ERROR "FDB_TEST_SERVER_SCRIPT is not defined. Please ensure the test server script is configured correctly.")
endif()

ecbuild_configure_file( catalogue.yaml.in catalogue.yaml @ONLY )
ecbuild_configure_file( store.yaml.in store.yaml @ONLY )
ecbuild_configure_file( client.yaml.in client.yaml @ONLY )

ecbuild_add_test(
    TARGET         FDB-491-remote-single-connection
    TYPE           SCRIPT
    COMMAND        ${FDB_TEST_SERVER_SCRIPT}
    ARGS           ${CMAKE_CURRENT_BINARY_DIR} client.yaml catalogue.yaml store.yaml
    TEST_DEPENDS   fdb_test_remote_api_bin
    RESOURCE_LOCK  fdb_remote_tests  # Prevent concurrent runs of remote tests
    RUN_SERIAL     TRUE
    LABELS         remotefdb
    ENVIRONMENT "${test_environment}"
)

if (TARGET FDB-491-remote-single-connection)
    set_tests_properties(FDB-491-remote-single-connection 
        PROPERTIES 
        TIMEOUT 300
    )
endif()
