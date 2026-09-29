function(add_py_test_marker_combo test_file config_marker op_marker)
    get_filename_component(stem ${test_file} NAME_WLE)

    add_test(
        NAME ${stem}_fdb_tools_${config_marker}_${op_marker}
        COMMAND ${Python_EXECUTABLE}
            -m pytest -m "${config_marker} and ${op_marker}"
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp/${stem}_${config_marker}_${op_marker}
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test_file}
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
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp/")
    set_tests_properties(${stem}_fdb_tools_${config_marker}_${op_marker}
        PROPERTIES
        ENVIRONMENT "${_env}"
        LABELS "fdb_tool_tests;fdb_tool_tests_${config_marker};fdb_tool_tests_${op_marker}"
    )
endfunction()

set(operation_markers info hide grib2fdb list purge read root wipe write overlay)

# config_markers selects the set of FdbEnvSpec IDs to exercise.
# All configs (subtoc and no-subtoc) are covered by a single run of
# test_fdb_tools.py — pytest_generate_tests in tool_tests/conftest.py
# generates only valid (spec, script) pairs so no combinations are skipped.
if(HAVE_PYTHON_EXTENDED_FDB_TOOL_TESTS)
    set(config_markers simple yaml files json yaml_mars_disks yaml_tools json_tools)
else()
    set(config_markers simple yaml)
endif()

foreach(config_marker ${config_markers})
    foreach(op_marker ${operation_markers})
        add_py_test_marker_combo(test_fdb_tools.py ${config_marker} ${op_marker})
    endforeach()
endforeach()
