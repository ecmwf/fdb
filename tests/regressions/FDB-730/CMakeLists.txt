if (NOT DEFINED FDB_TEST_SERVER_SCRIPT)
    message(FATAL_ERROR "FDB_TEST_SERVER_SCRIPT is not defined. Please ensure the test server script is configured correctly.")
endif()

ecbuild_configure_file( catalogue.yaml.in catalogue.yaml @ONLY )
ecbuild_configure_file( store.yaml.in store.yaml @ONLY )
ecbuild_configure_file( client.yaml.in client.yaml @ONLY )

ecbuild_add_executable(
    TARGET    fdb_730_regression_test
    SOURCES   fdb_730_regression_test.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      fdb5
    NOINSTALL
)

ecbuild_add_test(
    TARGET         FDB-730
    TYPE           SCRIPT
    COMMAND        ${FDB_TEST_SERVER_SCRIPT}
    ARGS           ${CMAKE_CURRENT_BINARY_DIR} client.yaml catalogue.yaml store.yaml "$<TARGET_FILE:fdb_730_regression_test>"
    TEST_DEPENDS   fdb_730_regression_test
    ENVIRONMENT    "${test_environment}"
    TEST_PROPERTIES
    TIMEOUT 90
    RESOURCE_LOCK fdb_remote_tests
    LABELS remotefdb
)
