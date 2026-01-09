include(parse_semver)
parse_semver(${CMAKE_CURRENT_SOURCE_DIR}/../../VERSION chunked_data_view)
message(STATUS "With 'chunked_data_view' [${chunked_data_view_VERSION_STRING}]")
configure_file(
    chunked_data_view_version.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/chunked_data_view_version.h
    @ONLY
)

add_library(chunked_data_view STATIC
    ${CMAKE_CURRENT_BINARY_DIR}/chunked_data_view_version.h
    Axis.cc
    Axis.h
    Buffer.cc
    ChunkedDataViewBuilder.cc
    ChunkedDataViewImpl.cc
    ChunkedDataViewImpl.h
    Extractor.cc
    Fdb.cc
    GribExtractor.cc
    GribExtractor.h
    IndexMapper.cc
    IndexMapper.h
    LibChunkedDataView.cc
    ListIterator.cc
    RequestManipulation.cc
    RequestManipulation.h
    ViewPart.cc
    ViewPart.h
    include/chunked_data_view/AxisDefinition.h
    include/chunked_data_view/Buffer.h
    include/chunked_data_view/ChunkedDataView.h
    include/chunked_data_view/ChunkedDataViewBuilder.h
    include/chunked_data_view/DataLayout.h
    include/chunked_data_view/Extractor.h
    include/chunked_data_view/Fdb.h
    include/chunked_data_view/LibChunkedDataView.h
    include/chunked_data_view/ListIterator.h
)

target_include_directories(chunked_data_view
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(chunked_data_view
    PRIVATE
        eckit
        metkit
        fdb5
)
