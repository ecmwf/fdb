pybind11_add_module(pychunked_data_view MODULE bindings.cc)

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
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
