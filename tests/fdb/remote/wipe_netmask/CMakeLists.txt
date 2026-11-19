ecbuild_add_executable(
    TARGET    fdb_test_remote_wipe_netmask
    SOURCES   test_wipe_netmask.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      fdb5
    NOINSTALL
)

if (HAVE_FDB_BUILD_TOOLS)
    ecbuild_configure_file( client_allowed.yaml.in client_allowed.yaml @ONLY )
    ecbuild_configure_file( catalogue_allowed.yaml.in catalogue_allowed.yaml @ONLY )
    ecbuild_configure_file( store_allowed.yaml.in store_allowed.yaml @ONLY )
    ecbuild_configure_file( client_denied.yaml.in client_denied.yaml @ONLY )
    ecbuild_configure_file( catalogue_denied.yaml.in catalogue_denied.yaml @ONLY )
    ecbuild_configure_file( store_denied.yaml.in store_denied.yaml @ONLY )
    ecbuild_configure_file( wipe_netmask.sh.in wipe_netmask.sh @ONLY )

    ecbuild_add_test(
        TARGET         fdb_test_remote_wipe_netmask_script
        TYPE           SCRIPT
        COMMAND        ${CMAKE_CURRENT_BINARY_DIR}/wipe_netmask.sh
        ARGS           ${CMAKE_CURRENT_BINARY_DIR}
                       client_allowed.yaml catalogue_allowed.yaml store_allowed.yaml
                       client_denied.yaml catalogue_denied.yaml store_denied.yaml
        TEST_DEPENDS   fdb_test_remote_wipe_netmask
        ENVIRONMENT    "${test_environment}"
        TEST_PROPERTIES
        TIMEOUT 300
        RESOURCE_LOCK fdb_remote_wipe_netmask
        LABELS remotefdb
    )
endif()
