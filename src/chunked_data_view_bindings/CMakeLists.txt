pybind11_add_module(chunked_data_view_bindings MODULE bindings.cc)

target_link_libraries(chunked_data_view_bindings
    PRIVATE pybind11::module 
            pybind11::lto 
            chunked_data_view
            eckit
            metkit
            fdb5
            # We need to explicity link to stdc++fs on gcc 8.x
            $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_GREATER_EQUAL:$<CXX_COMPILER_VERSION>,8.0>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,9.0>>:stdc++fs>
)

# pybind11_extension(pychunked_data_view)
if(NOT MSVC AND NOT ${CMAKE_BUILD_TYPE} MATCHES Debug|RelWithDebInfo)
    # Strip unnecessary sections of the binary on Linux/macOS
    pybind11_strip(chunked_data_view_bindings)
endif()

set_target_properties(chunked_data_view_bindings
  PROPERTIES 
  LIBRARY_OUTPUT_DIRECTORY ${Z3FDB_STAGING}/chunked_data_view_bindings
)
