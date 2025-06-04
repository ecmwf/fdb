
function(add_py_test test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME pyfdb_${stem} 
        COMMAND ${Python_INTERPRETER} 
            -m pytest 
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp 
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test}
    )
    set_property(
        TEST pyfdb_${stem} 
        PROPERTY 
        ENVIRONMENT 
            "PYTHONPATH=${CMAKE_BINARY_DIR}/lib:${CMAKE_CURRENT_SOURCE_DIR}/../../src"
            #prevent findlibs in pyfdb to pickup any globally installed fdb
            "FDB5_DIR=${CMAKE_BINARY_DIR}"
            #prevent findlibs in eccodes-python to pickup any globally installed eccodes
            "ECCODES_PYTHON_USE_FINDLIBS=1"
            "ECCODES_DIR=${CMAKE_BINARY_DIR}"
    ) 
endfunction()


set(test_files 
    unit/test_archive.py
    integration/test_passing_config_directly.py
    integration/test_archive_scenarios.py
    integration/test_pyfdb.py
)

foreach(test_file ${test_files})
  add_py_test(${test_file})
endforeach()
