configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/FDB-610.sh.in"
    "${CMAKE_CURRENT_BINARY_DIR}/FDB-610.sh.tmp"
    @ONLY
)

file(GENERATE
    OUTPUT FDB-610.sh
    INPUT "${CMAKE_CURRENT_BINARY_DIR}/FDB-610.sh.tmp"
)

file(CREATE_LINK
    ${CMAKE_CURRENT_SOURCE_DIR}/schema
    ${CMAKE_CURRENT_BINARY_DIR}/schema
    COPY_ON_ERROR
    SYMBOLIC)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/db_store)

ecbuild_add_test(
    TARGET         FDB-610
    TYPE           SCRIPT
    COMMAND        ${CMAKE_CURRENT_BINARY_DIR}/FDB-610.sh
    ENVIRONMENT    "${test_environment}"
)

set_tests_properties(FDB-610
    PROPERTIES
        RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
        RUN_SERIAL  TRUE
        LABELS remotefdb
)
