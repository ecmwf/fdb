set(_py_test_env
    "PYTHONPATH=${CMAKE_BINARY_DIR}/z3fdb-python-package-staging:${CMAKE_BINARY_DIR}/pyfdb-python-package-staging:${PYTHONPATH}"
    #prevent findlibs in pyfdb to pickup any globally installed fdb
    "FDB5_DIR=${CMAKE_BINARY_DIR}"
    #prevent findlibs in eccodes-python to pickup any globally installed eccodes
    "FINDLIBS_DISABLE_PACKAGE=yes"
    "ECCODES_DIR=$<TARGET_FILE_DIR:eccodes>/.."
    "ECCODES_PYTHON_USE_FINDLIBS=1"
)

function(add_py_test test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME pychunked_data_view_${stem}
        COMMAND ${Python_EXECUTABLE}
            -m pytest
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test}
    )
    set_tests_properties(pychunked_data_view_${stem}
        PROPERTIES
        RESOURCE_LOCK pytest
        ENVIRONMENT "${_py_test_env}"
        LABELS "z3fdb;z3fdb_offline"
    )
endfunction()

function(add_py_online_test test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME pychunked_data_view_${stem}
        COMMAND ${Python_EXECUTABLE}
            -m pytest
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test}
    )
    set_tests_properties(pychunked_data_view_${stem}
        PROPERTIES
        RESOURCE_LOCK pytest
        ENVIRONMENT "${_py_test_env}"
        LABELS "z3fdb;z3fdb_online"
    )
endfunction()


set(store_tests
  interface/store/test_store_v3_simple_builder.py
  interface/store/test_store_v3_errors_simple_builder.py
  interface/store/test_u_v_vo_d_retrieval_simple_builder.py
  interface/store/test_custom_store_builder.py
)

set(integration_tests
  integration/test_gribjump_extractor.py
  integration/test_mixed_extractors.py
)

set(zarr_interface_conformity_tests
  interface/zarr_interface_conformity/test_z3fdb_store_interface.py
  interface/zarr_interface_conformity/test_z3fdb_store_path_correctness.py
  interface/zarr_interface_conformity/test_z3fdb_consolidated_metadata.py
)

set(user_tests
  interface/user_tests/test_dask_access.py
  interface/user_tests/test_xarray_access.py
  interface/user_tests/test_metadata_mapping.py
)

set(permutation_tests
  permutation_tests/test_scrambled_request_ordering.py
  permutation_tests/test_axis_definition_ordering.py
  permutation_tests/test_axis_definition_permutations.py
  permutation_tests/test_store_fixed_size_chunking.py
  permutation_tests/test_builder_extension.py
  permutation_tests/test_store_multiple_parts.py
)

foreach(test_file ${store_tests} ${integration_tests} ${zarr_interface_conformity_tests} ${user_tests} ${permutation_tests})
  add_py_test(${test_file})
endforeach()

# Online tests: require network access to ECMWF open data.
# Run with: ctest -L z3fdb_online
add_py_online_test(permutation_tests/test_store_missing_values.py)
