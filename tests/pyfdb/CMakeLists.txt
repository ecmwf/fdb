
function(add_py_test test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME pyfdb_${stem} 
        COMMAND ${Python_INTERPRETER} 
            -m pytest 
            # NOTE: Parallel invocations of pytest require distinct basetemp paths per 
            #       invoked pytest instance. This is required to support ctest -j
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp/${stem}
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test}
    )
    set_property(
        TEST pyfdb_${stem} 
        PROPERTY 
        ENVIRONMENT 
            "PYTHONPATH=${CMAKE_BINARY_DIR}/lib:${CMAKE_CURRENT_SOURCE_DIR}/../../src/pyfdb"
            #prevent findlibs in pyfdb to pickup any globally installed fdb
            "FDB5_DIR=${CMAKE_BINARY_DIR}"
            #prevent findlibs in eccodes-python to pickup any globally installed eccodes
            "ECCODES_PYTHON_USE_FINDLIBS=1"
            "ECCODES_DIR=${CMAKE_BINARY_DIR}"
    ) 
endfunction()


set(test_files 
    tests/unit/test_archive.py
    tests/integration/test_passing_config_directly.py
    tests/integration/test_archive_scenarios.py
    tests/integration/test_pyfdb.py
)

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp")

foreach(test_file ${test_files})
  add_py_test(${test_file})
endforeach()
