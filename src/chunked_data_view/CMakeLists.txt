include(parse_semver)
parse_semver(${CMAKE_CURRENT_SOURCE_DIR}/VERSION chunked_data_view)
message(STATUS "With 'chunked_data_view' [${chunked_data_view_VERSION_STRING}]")
configure_file(
    chunked_data_view_version.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/chunked_data_view_version.h
    @ONLY
)

set(chunked_data_view_sources
    Axis.cc
    Axis.h
    ChunkedDataViewBuilder.cc
    ChunkedDataViewImpl.cc
    ChunkedDataViewImpl.h
    Buffer.cc
    Extractor.cc
    Fdb.cc
    GribExtractor.cc
    GribExtractor.h
    LibChunkedDataView.cc
    ListIterator.cc
    RequestManipulation.cc
    RequestManipulation.h
    IndexMapper.cc
    IndexMapper.h
    ViewPart.cc
    ViewPart.h
)

ecbuild_add_library(
    TARGET chunked_data_view
    TYPE STATIC
    INSTALL_HEADERS_LIST
        include/chunked_data_view/AxisDefinition.h
        include/chunked_data_view/Buffer.h
        include/chunked_data_view/ChunkedDataView.h
        include/chunked_data_view/ChunkedDataViewBuilder.h
        include/chunked_data_view/DataLayout.h
        include/chunked_data_view/Extractor.h
        include/chunked_data_view/LibChunkedDataView.h
        include/chunked_data_view/ListIterator.h
    HEADER_DESTINATION
        ${INSTALL_INCLUDE_DIR}/chunked_data_view
    SOURCES
        ${chunked_data_view_sources}
    PUBLIC_INCLUDES
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    PRIVATE_INCLUDES
        ${CMAKE_CURRENT_BINARY_DIR}
    PRIVATE_LIBS
        eckit
        metkit
        fdb5
)
