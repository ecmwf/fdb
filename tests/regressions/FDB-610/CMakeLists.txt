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

add_test(NAME test_610
         COMMAND ${CMAKE_CURRENT_BINARY_DIR}/FDB-610.sh)

set_tests_properties(test_610
    PROPERTIES
        ENVIRONMENT "${test_environment}"
        RESOURCE_LOCK fdb_remote_tests  # Prevent concurrent runs of remote tests
        RUN_SERIAL  TRUE
        LABELS remotefdb
        )
