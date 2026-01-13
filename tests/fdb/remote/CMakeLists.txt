# Client side of test
ecbuild_add_executable(
    TARGET    fdb_test_remote_api_bin
    SOURCES   test_remote_api.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      fdb5
    NOINSTALL
)

if (HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB)
    ecbuild_configure_file( test_server.sh.in fdb_test_server.sh @ONLY )
    set(FDB_TEST_SERVER_SCRIPT "${CMAKE_CURRENT_BINARY_DIR}/fdb_test_server.sh" CACHE INTERNAL "")

    # Basic API test using "standard" configuration
    ecbuild_configure_file( catalogue.yaml.in catalogue.yaml @ONLY )
    ecbuild_configure_file( store.yaml.in store.yaml @ONLY )
    ecbuild_configure_file( client.yaml.in client.yaml @ONLY )

    ecbuild_add_test(
        TARGET         fdb_test_remote_api
        TYPE           SCRIPT
        COMMAND        ${FDB_TEST_SERVER_SCRIPT}
        ARGS           ${CMAKE_CURRENT_BINARY_DIR} client.yaml catalogue.yaml store.yaml
        TEST_DEPENDS   fdb_test_remote_api_bin
        ENVIRONMENT    "${test_environment}"
        RESOURCE_LOCK  fdb_remote_tests  # Prevent concurrent runs of remote tests
        RUN_SERIAL     TRUE
        LABELS         remotefdb
    )
    # if (TARGET fdb_test_remote_api)
        set_tests_properties(fdb_test_remote_api
            PROPERTIES TIMEOUT 300
            RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
            RUN_SERIAL  TRUE
            LABELS remotefdb
        )
    # endif()
endif()

