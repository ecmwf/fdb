function(add_py_test test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME pychunked_data_view_${stem}
        COMMAND ${Python_EXECUTABLE}
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
        "ECCODES_PYTHON_USE_FINDLIBS=1"
    )
    set_tests_properties(pychunked_data_view_${stem}
        PROPERTIES
        RESOURCE_LOCK pytest
        ENVIRONMENT "${_env}"
        LABELS "z3fdb;z3fdb_offline"
    )
endfunction()


set(unit_tests
  unit/test_store_v3.py
  unit/test_store_v3_errors.py
  unit/test_u_v_vo_d_retrieval.py
)

set(zarr_interface_conformity_tests
  zarr_interface_conformity/test_z3fdb_store_interface.py
  zarr_interface_conformity/test_z3fdb_store_path_correctness.py
  zarr_interface_conformity/test_z3fdb_consolidated_metadata.py
)

set(permutation_tests
  permutation_tests/test_scrambled_request_ordering.py
  permutation_tests/test_axis_definition_ordering.py
  permutation_tests/test_axis_definition_permutations.py
  permutation_tests/test_store_individual_chunking.py
  permutation_tests/test_store_multiple_parts.py
)

foreach(test_file ${unit_tests} ${zarr_interface_conformity_tests} ${permutation_tests})
  add_py_test(${test_file})
endforeach()

# Online tests: require network access to ECMWF open data.
# Run with: ctest -L z3fdb_online
get_filename_component(online_stem test_store_v3_online.py NAME_WLE)
add_test(
    NAME pychunked_data_view_${online_stem}
    COMMAND ${Python_EXECUTABLE}
        -m pytest
        --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp
        -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/test_store_v3_online.py
)
set(_env
    "PYTHONPATH=${CMAKE_BINARY_DIR}/z3fdb-python-package-staging:${CMAKE_BINARY_DIR}/pyfdb-python-package-staging:${PYTHONPATH}"
    #prevent findlibs in pyfdb to pickup any globally installed fdb
    "FDB5_DIR=${CMAKE_BINARY_DIR}"
    #prevent findlibs in eccodes-python to pickup any globally installed eccodes
    "FINDLIBS_DISABLE_PACKAGE=yes"
    "ECCODES_DIR=$<TARGET_FILE_DIR:eccodes>/.."
    "ECCODES_PYTHON_USE_FINDLIBS=1"
)
set_tests_properties(pychunked_data_view_${online_stem}
    PROPERTIES
    RESOURCE_LOCK pytest
    ENVIRONMENT "${_env}"
    LABELS "z3fdb;z3fdb_online"
)
