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
        "ECCODES_DIR=${CMAKE_BINARY_DIR}"
    )
    set_tests_properties(pyfdb_${stem}
        PROPERTIES
        RESOURCE_LOCK pytest
        ENVIRONMENT "${_env}"
        LABELS pyfdb
    )
endfunction()


set(test_files
    unit/test_archive.py
    unit/test_control.py
    unit/test_data_handle.py
    unit/test_enums.py
    unit/test_fdb.py
    unit/test_identifier.py
    unit/test_index_axis.py
    unit/test_initialization.py
    unit/test_inspect.py
    unit/test_list.py
    unit/test_mars_request.py
    unit/test_move.py
    unit/test_purge.py
    unit/test_retrieve.py
    unit/test_stats.py
    unit/test_status.py
    unit/test_types.py
    unit/test_uri.py
    unit/test_wipe.py
)

foreach(test_file ${test_files})
    add_py_test(${test_file})
endforeach()

