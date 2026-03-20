# Set the labels to the simple and yaml config subset for the 'default' tests
if(HAVE_PYTHON_EXTENDED_FDB_TOOL_TESTS) 
    set(labels "")
else()
    set(labels "-m (simple or yaml)")
endif()

message("Labels for the execution of e2e tests: ${labels}")

function(add_py_test test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME fdb_tool_tests_${stem}
        COMMAND ${Python_EXECUTABLE}
            -m pytest ${labels}
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test}
    )
    set(_env
        "PYTHONPATH=${CMAKE_BINARY_DIR}/pyfdb-python-package-staging:$ENV{PYTHONPATH}"
        #prevent findlibs in pyfdb to pickup any globally installed fdb
        "PATH=${CMAKE_BINARY_DIR}/bin:$ENV{PATH}"
        "FDB5_DIR=${CMAKE_BINARY_DIR}"
        #prevent findlibs in eccodes-python to pickup any globally installed eccodes
        "ECCODES_PYTHON_USE_FINDLIBS=1"
        "ECCODES_DIR=$<TARGET_FILE_DIR:eccodes>/.."
    )
    set_tests_properties(fdb_tool_tests_${stem}
        PROPERTIES
        RESOURCE_LOCK pytest
        ENVIRONMENT "${_env}"
        LABELS fdb_tool_tests
    )
endfunction()

set(test_files
    test_fdb_no_subtocs.py    
    test_fdb_subtocs.py    
)

foreach(test_file ${test_files})
    add_py_test(${test_file})
endforeach()
