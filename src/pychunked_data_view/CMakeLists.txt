find_package(Python 3.11 COMPONENTS Interpreter Development REQUIRED)

pybind11_add_module(pychunked_data_view MODULE src/bindings.cc)

target_link_libraries(pychunked_data_view 
    PRIVATE pybind11::module 
            pybind11::lto 
            chunked_data_view
            eckit)

# pybind11_extension(pychunked_data_view)
if(NOT MSVC AND NOT ${CMAKE_BUILD_TYPE} MATCHES Debug|RelWithDebInfo)
    # Strip unnecessary sections of the binary on Linux/macOS
    pybind11_strip(pychunked_data_view)
endif()

set_target_properties(pychunked_data_view 
  PROPERTIES 
    CXX_VISIBILITY_PRESET "hidden" 
    CUDA_VISIBILITY_PRESET "hidden"
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
  )

install(
  TARGETS pychunked_data_view 
  COMPONENT python
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")

function(add_py_test test)
  get_filename_component(stem ${test} NAME_WLE)
  add_test(
    NAME pychunked_data_view_${stem} 
    COMMAND ${Python_INTERPRETER} -m pytest --basetemp=${CMAKE_CURRENT_BINBARY_DIR}/pytest-tmp -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/test/${test}
  )
  set_property(TEST pychunked_data_view_${stem} PROPERTY ENVIRONMENT "PYTHONPATH=${CMAKE_BINARY_DIR}/lib:${CMAKE_CURRENT_SOURCE_DIR}/python")
endfunction()


set(test_files test_setup.py)

foreach(test_file ${test_files})
  add_py_test(${test_file})
endforeach()
