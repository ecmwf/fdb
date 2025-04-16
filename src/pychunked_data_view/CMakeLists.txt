add_test(
  NAME pychunked_data_view_ensure_deps
  COMMAND ${Python_INTERPRETER} -m pip install pytest
)


function(add_py_test test)
  get_filename_component(stem ${test} NAME_WLE)
  add_test(
    NAME pychunked_data_view_${stem} 
    COMMAND ${Python_INTERPRETER} -m pytest --basetemp=${CMAKE_CURRENT_BINBARY_DIR}/pytest-tmp -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/test/${test}
  )
  set_property(
    TEST pychunked_data_view_${stem} 
    PROPERTY 
      ENVIRONMENT "PYTHONPATH=${CMAKE_BINARY_DIR}/lib:${CMAKE_CURRENT_SOURCE_DIR}"
      DEPENDS pychunked_data_view_ensure_deps
  )
endfunction()


set(test_files 
  test_setup.py
)

foreach(test_file ${test_files})
  add_py_test(${test_file})
endforeach()
