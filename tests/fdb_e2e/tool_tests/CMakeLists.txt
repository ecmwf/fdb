# Convert a pytest test ID into a CMP0110-safe test name
function(sanitize_test_name INPUT OUTPUT)
    set(name "${INPUT}")

    # Collapse multiple underscores
    string(REGEX REPLACE ":" "_" name "${name}")

    # Square brackets
    string(REGEX REPLACE "[\]\[]" "_" name "${name}")

    # Collapse multiple underscores
    string(REGEX REPLACE "_+" "_" name "${name}")

    # Remove leading/trailing underscores
    string(REGEX REPLACE "^_|_$" "" name "${name}")

    set(${OUTPUT} "${name}" PARENT_SCOPE)
endfunction()


execute_process(
     COMMAND ${CMAKE_COMMAND} -E env
         "PYTHONPATH=${CMAKE_BINARY_DIR}/pyfdb-python-package-staging:$ENV{PYTHONPATH}"
         pytest --collect-only -q --rootdir=${CMAKE_CURRENT_SOURCE_DIR}
     WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
     OUTPUT_VARIABLE PYTEST_TESTS
     OUTPUT_STRIP_TRAILING_WHITESPACE
)

string(REPLACE "\n" ";" PYTEST_TEST_LIST "${PYTEST_TESTS}")

if(PYTEST_TEST_LIST STREQUAL "" OR PYTEST_TEST_LIST STREQUAL ";")
    message(FATAL_ERROR "Pytest did not collect any e2e tests. Check the setup.")
endif()

foreach(test ${PYTEST_TEST_LIST})
    if(test MATCHES "::")
        sanitize_test_name("${test}" test_name)

        add_test(NAME "${test_name}"
                 COMMAND pytest -q "${test}" --rootdir=${CMAKE_CURRENT_SOURCE_DIR}
                 WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
        set(_env
            "PYTHONPATH=${CMAKE_BINARY_DIR}/pyfdb-python-package-staging:$ENV{PYTHONPATH}"
            #prevent findlibs in pyfdb to pickup any globally installed fdb
            "PATH=${CMAKE_BINARY_DIR}/bin:$ENV{PATH}"
            #prevent findlibs in eccodes-python to pickup any globally installed eccodes
            "ECCODES_PYTHON_USE_FINDLIBS=1"
            "ECCODES_DIR=$<TARGET_FILE_DIR:eccodes>/.."
        )
        set_tests_properties(${test_name}
            PROPERTIES
            ENVIRONMENT "${_env}"
            LABELS fdb_tool_tests
        )
    endif()
endforeach()
