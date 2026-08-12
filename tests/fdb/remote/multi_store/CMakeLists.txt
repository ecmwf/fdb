ecbuild_add_executable(
    TARGET    fdb_test_remote_multi_store
    SOURCES   test_multi_store.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      fdb5
    NOINSTALL
)

if (HAVE_FDB_BUILD_TOOLS)
    ecbuild_configure_file( client.yaml.in client.yaml @ONLY )
    ecbuild_configure_file( store1.yaml.in store1.yaml @ONLY )
    ecbuild_configure_file( store2.yaml.in store2.yaml @ONLY )
    ecbuild_configure_file( multi_store.sh.in multi_store.sh @ONLY )

    ecbuild_add_test(
        TARGET         fdb_test_remote_multi_store_script
        TYPE           SCRIPT
        COMMAND        ${CMAKE_CURRENT_BINARY_DIR}/multi_store.sh
        ARGS           ${CMAKE_CURRENT_BINARY_DIR} client.yaml store1.yaml store2.yaml
        TEST_DEPENDS   fdb_test_remote_multi_store
        ENVIRONMENT    "${test_environment}"
        TEST_PROPERTIES
        TIMEOUT 600
        RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
        LABELS remotefdb
    )

    ecbuild_configure_file( multi_store_tools.sh.in multi_store_tools.sh @ONLY )

    ecbuild_add_test(
        TARGET         fdb_test_remote_multi_store_tools
        TYPE           SCRIPT
        CONDITION      HAVE_GRIB
        COMMAND        ${CMAKE_CURRENT_BINARY_DIR}/multi_store_tools.sh
        ARGS           ${CMAKE_CURRENT_BINARY_DIR} client.yaml store1.yaml store2.yaml ${PROJECT_BINARY_DIR}/tests/fdb/od.oper.grib
        TEST_DEPENDS   fdb_test_remote_multi_store fdb_test_get_data
        ENVIRONMENT    "${test_environment}"
        TEST_PROPERTIES
        TIMEOUT 600
        RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
        LABELS remotefdb
    )
endif()
