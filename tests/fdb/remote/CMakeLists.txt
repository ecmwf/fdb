# Protocol negotiation (no server needed)
ecbuild_add_test(
    TARGET    fdb_test_remote_negotiation
    SOURCES   test_remote_negotiation.cc
    LIBS      fdb5
)

# Client side of test
ecbuild_add_executable(
    TARGET    fdb_test_remote_api_bin
    SOURCES   test_remote_api.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      fdb5
    NOINSTALL
)

if (HAVE_FDB_BUILD_TOOLS)
    ecbuild_configure_file( fdb_remote_test_helper.sh.in fdb_remote_test_helper.sh @ONLY )
    set(FDB_REMOTE_TEST_HELPER "${CMAKE_CURRENT_BINARY_DIR}/fdb_remote_test_helper.sh" CACHE INTERNAL "")
endif()

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
        TEST_PROPERTIES
        TIMEOUT 300
        RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
        LABELS remotefdb
    )

    # A request waiting for a server must be woken up, not resumed, when the connection is lost and the server restarts
    ecbuild_add_executable(
        TARGET    fdb_test_remote_connection_restart_bin
        SOURCES   test_remote_connection_restart.cc
        INCLUDES  ${ECKIT_INCLUDE_DIRS}
        LIBS      fdb5
        NOINSTALL
    )

    ecbuild_add_test(
        TARGET         fdb_test_remote_connection_restart
        TYPE           SCRIPT
        COMMAND        ${FDB_TEST_SERVER_SCRIPT}
        ARGS           ${CMAKE_CURRENT_BINARY_DIR} client.yaml catalogue.yaml store.yaml "$<TARGET_FILE:fdb_test_remote_connection_restart_bin>"
        TEST_DEPENDS   fdb_test_remote_connection_restart_bin
        ENVIRONMENT    "${test_environment}"
        TEST_PROPERTIES
        TIMEOUT 120
        RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
        LABELS remotefdb
    )

    # End-to-end trace-context propagation: client and servers with tracing enabled, or disabled on either side
    ecbuild_add_executable(
        TARGET    fdb_test_remote_tracing_bin
        SOURCES   test_remote_tracing.cc
        INCLUDES  ${ECKIT_INCLUDE_DIRS}
        LIBS      fdb5
        NOINSTALL
    )

    foreach( _variant enabled client_disabled server_disabled )
        set( TRACING_VARIANT ${_variant} )
        set( TRACING_CLIENT true )
        set( TRACING_SERVER true )
        if( _variant STREQUAL "client_disabled" )
            set( TRACING_CLIENT false )
        elseif( _variant STREQUAL "server_disabled" )
            set( TRACING_SERVER false )
        endif()

        ecbuild_configure_file( tracing_client.yaml.in    tracing_${_variant}_client.yaml    @ONLY )
        ecbuild_configure_file( tracing_catalogue.yaml.in tracing_${_variant}_catalogue.yaml @ONLY )
        ecbuild_configure_file( tracing_store.yaml.in     tracing_${_variant}_store.yaml     @ONLY )

        ecbuild_add_test(
            TARGET         fdb_test_remote_tracing_${_variant}
            TYPE           SCRIPT
            COMMAND        ${FDB_TEST_SERVER_SCRIPT}
            ARGS           ${CMAKE_CURRENT_BINARY_DIR} tracing_${_variant}_client.yaml tracing_${_variant}_catalogue.yaml
                           tracing_${_variant}_store.yaml "$<TARGET_FILE:fdb_test_remote_tracing_bin>"
            TEST_DEPENDS   fdb_test_remote_tracing_bin
            ENVIRONMENT    "${test_environment}" "FDB_TRACING_TEST_VARIANT=${_variant}"
                           "FDB_TRACING_TEST_DIR=${CMAKE_CURRENT_BINARY_DIR}"
            TEST_PROPERTIES
            TIMEOUT 120
            RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
            LABELS remotefdb
        )
    endforeach()

    # See, https://jira.ecmwf.int/browse/FDB-720
    # Regression test for the control/data connection message-ordering race in RemoteStore::handle()
    # ecbuild_add_executable(
    #     TARGET    fdb_test_remote_store_race_bin
    #     SOURCES   test_remote_store_race.cc
    #     INCLUDES  ${ECKIT_INCLUDE_DIRS}
    #     LIBS      fdb5
    #     NOINSTALL
    # )
    #
    # ecbuild_add_test(
    #     TARGET         fdb_test_remote_store_race
    #     TYPE           SCRIPT
    #     COMMAND        ${FDB_TEST_SERVER_SCRIPT}
    #     ARGS           ${CMAKE_CURRENT_BINARY_DIR} client.yaml catalogue.yaml store.yaml "$<TARGET_FILE:fdb_test_remote_store_race_bin>"
    #     TEST_DEPENDS   fdb_test_remote_store_race_bin
    #     ENVIRONMENT    "${test_environment}"
    #     TEST_PROPERTIES
    #     TIMEOUT 120
    #     RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
    #     LABELS remotefdb
    # )
endif()

add_subdirectory( multi_store )

if("${ECKIT_VERSION}" VERSION_GREATER_EQUAL 2.2)
    add_subdirectory( single_conn_stress )
endif()