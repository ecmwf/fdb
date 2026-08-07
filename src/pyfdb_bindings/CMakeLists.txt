pybind11_add_module(pyfdb_bindings MODULE bindings.cc)

target_link_libraries(pyfdb_bindings
    PRIVATE pybind11::module 
            pybind11::lto 
            chunked_data_view
            eckit
            metkit
            fdb5
            # We need to explicity link to stdc++fs on gcc 8.x
            $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_GREATER_EQUAL:$<CXX_COMPILER_VERSION>,8.0>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,9.0>>:stdc++fs>
)

if(NOT MSVC AND NOT ${CMAKE_BUILD_TYPE} MATCHES Debug|RelWithDebInfo)
    # Strip unnecessary sections of the binary on Linux/macOS
    pybind11_strip(pyfdb_bindings)
endif()

set_target_properties(pyfdb_bindings
  PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY ${PYFDB_STAGING}/pyfdb_bindings
)

add_custom_command(TARGET pyfdb_bindings POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${CMAKE_CURRENT_SOURCE_DIR}/__init__.py
            $<TARGET_FILE_DIR:pyfdb_bindings>
    COMMENT "Copying __init__.py for pyfdb_bindings"
)

