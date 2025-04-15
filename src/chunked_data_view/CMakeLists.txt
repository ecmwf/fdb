include(parse_semver)
parse_semver(${CMAKE_CURRENT_SOURCE_DIR}/VERSION chunked_data_view)
message(STATUS "With 'chunked_data_view' [${chunked_data_view_VERSION_STRING}]")
configure_file(
    chunked_data_view_version.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/chunked_data_view_version.h
    @ONLY
)

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
