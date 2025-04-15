include(parse_semver)
parse_semver(${CMAKE_CURRENT_SOURCE_DIR}/VERSION chunked_data_view)
message(STATUS "With 'chunked_data_view' [${chunked_data_view_VERSION_STRING}]")
configure_file(
    chunked_data_view_version.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/chunked_data_view_version.h
    @ONLY
)

add_library(pychunked_data_view SHARED src/chunked_data_view/example.cpp)

target_link_libraries(pychunked_data_view 
    PRIVATE pybind11::module 
            pybind11::lto 
            chunked_data_view
            eckit)

pybind11_extension(pychunked_data_view)
if(NOT MSVC AND NOT ${CMAKE_BUILD_TYPE} MATCHES Debug|RelWithDebInfo)
    # Strip unnecessary sections of the binary on Linux/macOS
    pybind11_strip(pychunked_data_view)
endif()

set_target_properties(pychunked_data_view PROPERTIES CXX_VISIBILITY_PRESET "hidden"
                                         CUDA_VISIBILITY_PRESET "hidden")

install(TARGETS pychunked_data_view COMPONENT python
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")

ecbuild_add_library(
    TARGET chunked_data_view
    TYPE SHARED
    INSTALL_HEADERS_LIST
        include/chunked_data_view/chunked_data_view.h
    HEADER_DESTINATION
        ${INSTALL_INCLUDE_DIR}/chunked_data_view
    SOURCES
        chunked_data_view.cc
    PUBLIC_INCLUDES
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    PRIVATE_INCLUDES
        ${CMAKE_CURRENT_BINARY_DIR}
    PRIVATE_LIBS
        eckit
        fdb5
)
