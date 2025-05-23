
ecbuild_configure_file( test_server.sh.in fdb_test_server.sh @ONLY )

# could configure these with a loop or wildcard
ecbuild_configure_file( fdb_catalogue.yaml.in fdb_catalogue.yaml @ONLY )
ecbuild_configure_file( fdb_store.yaml.in fdb_store.yaml @ONLY )
ecbuild_configure_file( fdb_client.yaml.in fdb_client.yaml @ONLY )

# Client side of test
ecbuild_add_executable(
    TARGET    fdb_test_remote_exe
    SOURCES   test_remote.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
    NOINSTALL
)

# Wrapper script to start the server and run the client test
ecbuild_add_test(
    TARGET    fdb_test_remote
    TYPE      SCRIPT
    COMMAND   fdb_test_server.sh
    TEST_DEPENDS fdb_test_remote_exe
    LABELS    remote
)
