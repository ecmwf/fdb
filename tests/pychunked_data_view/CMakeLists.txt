function(add_py_test test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME pychunked_data_view_${stem}
        COMMAND ${Python_INTERPRETER}
            -m pytest
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test}
    )
    set(_env
        "PYTHONPATH=${CMAKE_BINARY_DIR}/z3fdb-python-package-staging:${CMAKE_BINARY_DIR}/pyfdb-python-package-staging:${PYTHONPATH}"
        #prevent findlibs in pyfdb to pickup any globally installed fdb
        "FDB5_DIR=${CMAKE_BINARY_DIR}"
        #prevent findlibs in eccodes-python to pickup any globally installed eccodes
        "FINDLIBS_DISABLE_PACKAGE=yes"
        "ECCODES_DIR=$<TARGET_FILE_DIR:eccodes>/.."
    )
    set_tests_properties(pychunked_data_view_${stem}
        PROPERTIES
        RESOURCE_LOCK pytest
        ENVIRONMENT "${_env}"
        LABELS z3fdb
    )
endfunction()


set(test_files
    test_chunked_data_view.py
)

foreach(test_file ${test_files})
    add_py_test(${test_file})
endforeach()
