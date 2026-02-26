function(add_py_test test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME pyfdb_${stem}
        COMMAND ${Python_INTERPRETER}
            -m pytest
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test}
    )
    set(_env
        "PYTHONPATH=${CMAKE_BINARY_DIR}/pyfdb-python-package-staging:${PYTHONPATH}"
        #prevent findlibs in pyfdb to pickup any globally installed fdb
        "FDB5_DIR=${CMAKE_BINARY_DIR}"
        #prevent findlibs in eccodes-python to pickup any globally installed eccodes
        "ECCODES_PYTHON_USE_FINDLIBS=1"
        "ECCODES_DIR=$<TARGET_FILE_DIR:eccodes>/.."
    )
    set_tests_properties(pyfdb_${stem}
        PROPERTIES
        RESOURCE_LOCK pytest
        ENVIRONMENT "${_env}"
        LABELS pyfdb
    )
endfunction()

file(GLOB test_files integration/ *.py)

foreach(test_file ${test_files})
    add_py_test(${test_file})
endforeach()

# Testing of pyfdb docs
function(add_rst_doc_test test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME pyfdb_doc_${stem}
        COMMAND ${Python_INTERPRETER}
            -m pytest
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test}
    )
    set(_env
        "PYTHONPATH=${CMAKE_BINARY_DIR}/pyfdb-python-package-staging:${PYTHONPATH}"
        #prevent findlibs in pyfdb to pickup any globally installed fdb
        "FDB5_DIR=${CMAKE_BINARY_DIR}"
        #prevent findlibs in eccodes-python to pickup any globally installed eccodes
        "ECCODES_PYTHON_USE_FINDLIBS=1"
        "ECCODES_DIR=$<TARGET_FILE_DIR:eccodes>/.."
    )
    set_tests_properties(pyfdb_doc_${stem}
        PROPERTIES
        RESOURCE_LOCK pytest
        ENVIRONMENT "${_env}"
        LABELS pyfdb_doc
    )
endfunction()


set(doc_test_files
    ../../docs/pyfdb/examples.rst
)

foreach(test_file ${doc_test_files})
    add_rst_doc_test(${test_file})
endforeach()
