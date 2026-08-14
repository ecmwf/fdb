if (HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB)
    ecbuild_add_executable(
        TARGET    fdb_test_remote_single_conn_stress_bin
        SOURCES   test_single_conn_stress.cc
        INCLUDES  ${ECKIT_INCLUDE_DIRS}
        LIBS      fdb5
        NOINSTALL
    )

    ecbuild_configure_file( catalogue.yaml.in catalogue.yaml @ONLY )
    ecbuild_configure_file( store.yaml.in store.yaml @ONLY )
    ecbuild_configure_file( client.yaml.in client.yaml @ONLY )
    ecbuild_configure_file( tsan_suppressions.txt.in tsan_suppressions.txt @ONLY )
    ecbuild_configure_file( test_server.sh.in fdb_test_single_conn_stress_server.sh @ONLY )

    ecbuild_add_test(
        TARGET         fdb_test_remote_single_conn_stress
        TYPE           SCRIPT
        COMMAND        ${CMAKE_CURRENT_BINARY_DIR}/fdb_test_single_conn_stress_server.sh
        ARGS           ${CMAKE_CURRENT_BINARY_DIR} client.yaml catalogue.yaml store.yaml
        TEST_DEPENDS   fdb_test_remote_single_conn_stress_bin
        ENVIRONMENT    "${test_environment}"
                       "TEST_FDB_THREAD_COUNT=8"
                       "TEST_FDB_STRESS_ITERATIONS=20"
                       "FDB_WIPE_ENABLED=1"
        TEST_PROPERTIES
        TIMEOUT 300
        RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
        LABELS remotefdb
    )
endif()
