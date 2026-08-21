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
    ChunkedDataViewBuilder.cc
    ChunkedDataViewImpl.cc
    ChunkedDataViewImpl.h
    Fdb.cc
    extractors/grib/GribExtractor.cc
    extractors/grib/GribExtractor.h
    extractors/grib/GribExtractorDefinition.cc
    extractors/grib/GribExtractorDefinition.h
    extractors/gribjump/GribJumpExtractorDefinition.cc
    extractors/gribjump/GribJumpExtractorDefinition.h
    LibChunkedDataView.cc
    ListIterator.cc
    RequestManipulation.cc
    RequestManipulation.h
    ViewPart.cc
    ViewPart.h
    mapping/AxisMapper.cc
    mapping/AxisMapper.h
    mapping/IndexMapper.cc
    mapping/IndexMapper.h
    exception/GribExtractorException.cc
    exception/GribJumpExtractorException.cc
    exception/UnknownExtractorException.cc
    exception/BoundingBoxException.cc
    exception/RequestManipulationException.cc
    exception/AxisMapperException.cc
    include/chunked_data_view/AxisDefinition.h
    include/chunked_data_view/ChunkedDataView.h
    include/chunked_data_view/ChunkedDataViewBuilder.h
    include/chunked_data_view/DataLayout.h
    include/chunked_data_view/Extractor.h
    include/chunked_data_view/Fdb.h
    include/chunked_data_view/LibChunkedDataView.h
    include/chunked_data_view/ListIterator.h
    include/chunked_data_view/Types.h
    include/chunked_data_view/exception/GribExtractorException.h
    include/chunked_data_view/exception/GribJumpExtractorException.h
    include/chunked_data_view/exception/UnknownExtractorException.h
    include/chunked_data_view/exception/BoundingBoxException.h
    include/chunked_data_view/exception/RequestManipulationException.h
    include/chunked_data_view/exception/AxisMapperException.h
)

target_include_directories(chunked_data_view
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}
)

# GribJumpExtractor is the only part of this library that needs gribjump headers. The
# definition (GribJumpExtractorDefinition) and its exception are gribjump-free and stay
# unconditional, so the type remains complete — and bindable from Python — in every build.
if(HAVE_ZARR_GRIBJUMP_EXTRACTOR)
    target_sources(chunked_data_view PRIVATE
        extractors/gribjump/GribJumpExtractor.cc
        extractors/gribjump/GribJumpExtractor.h
    )
    target_link_libraries(chunked_data_view PRIVATE gribjump)
    # PUBLIC: GribJumpExtractorDefinition.cc and chunked_data_view_bindings both switch on this.
    target_compile_definitions(chunked_data_view PUBLIC HAVE_ZARR_GRIBJUMP_EXTRACTOR)
endif()

target_link_libraries(chunked_data_view
    PRIVATE
        eckit
        metkit
        fdb5
)
