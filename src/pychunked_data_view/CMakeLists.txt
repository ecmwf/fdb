function(add_py_test test)
  get_filename_component(stem ${test} NAME_WLE)
  add_test(
    NAME pychunked_data_view_${stem} 
    COMMAND ${Python_INTERPRETER} 
        -m pytest 
        --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp 
        -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/test/${test}
  )
  set_property(
    TEST pychunked_data_view_${stem} 
    PROPERTY 
      ENVIRONMENT "PYTHONPATH=${CMAKE_BINARY_DIR}/lib:${CMAKE_CURRENT_SOURCE_DIR}")
endfunction()


set(test_files 
  test_chunked_data_view.py
)

foreach(test_file ${test_files})
  add_py_test(${test_file})
endforeach()
