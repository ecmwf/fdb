# Create the pytest-tmp folder for encompassing the different scenarios
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp")

function(add_pyfdb_test scenario test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME ${scenario}_${stem}
        COMMAND ${Python_EXECUTABLE}
            -m pytest
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp/${scenario}/
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test}
    )
    set(_env
        "PYTHONPATH=${CMAKE_BINARY_DIR}/pyfdb-python-package-staging:${PYTHONPATH}"
        #prevent findlibs in pyfdb to pickup any globally installed fdb
        "FDB5_DIR=${CMAKE_BINARY_DIR}"
        #prevent findlibs in eccodes-python to pickup any globally installed eccodes
        "ECCODES_PYTHON_USE_FINDLIBS=1"
        "ECCODES_DIR=$<TARGET_FILE_DIR:eccodes>/.."
        "FINDLIBS_DISABLE_PACKAGE=yes"
    )
    set_tests_properties(${scenario}_${stem}
        PROPERTIES
        RESOURCE_LOCK ${scenario}
        ENVIRONMENT "${_env}"
        LABELS ${scenario}
    )
endfunction()

set(test_files
    integration/test_archive.py
    integration/test_config.py
    integration/test_config_mapper.py
    integration/test_control.py
    integration/test_control_enums.py
    integration/test_data_handle.py
    integration/test_fdb.py
    integration/test_fdb_tool_request.py
    integration/test_index_axis.py
    integration/test_inspect.py
    integration/test_list.py
    integration/test_purge.py
    integration/test_retrieve.py
    integration/test_selection_mapper.py
    integration/test_stats.py
    integration/test_status.py
    integration/test_types.py
    integration/test_uri.py
    integration/test_wildcard_selection.py
    integration/test_wipe.py
)

foreach(test_file ${test_files})
    add_pyfdb_test("pyfdb" ${test_file})
endforeach()

set(doc_test_files
    ../../docs/pyfdb/examples.rst
)

foreach(test_file ${doc_test_files})
    add_pyfdb_test("pyfdb_doc" ${test_file})
endforeach()
