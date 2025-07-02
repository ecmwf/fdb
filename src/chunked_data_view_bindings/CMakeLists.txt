pybind11_add_module(chunked_data_view_bindings MODULE bindings.cc)

target_link_libraries(chunked_data_view_bindings
    PRIVATE pybind11::module 
            pybind11::lto 
            chunked_data_view
            eckit
            metkit
            fdb5
)

# pybind11_extension(pychunked_data_view)
if(NOT MSVC AND NOT ${CMAKE_BUILD_TYPE} MATCHES Debug|RelWithDebInfo)
    # Strip unnecessary sections of the binary on Linux/macOS
    pybind11_strip(chunked_data_view_bindings)
endif()

set_target_properties(chunked_data_view_bindings
  PROPERTIES 
  #CXX_VISIBILITY_PRESET "hidden" 
  # CUDA_VISIBILITY_PRESET "hidden"
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
  )

install(
  TARGETS chunked_data_view_bindings
  COMPONENT python
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
