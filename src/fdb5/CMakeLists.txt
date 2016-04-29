### TODO
#
#  * Axis should consider Type
#
#  * client "database" for direct fdb5 read
#
#  * fdb5_list -class od -expver 0001
#  * fdb5_list .
#  * fdb5_purge -class rd -expver ghtf
#
#  * consider using more than a service node on the HPC
#
#  * marsadm tools:
#    --  list roots
#

# TESTS:
# check effect on retrieve:
#  * delete a json
#  * delete a data
#  * delete a index

# MAYBE:
#  * fdbRoot is a list of paths to visit ---> speak to Hodge
#  * Rules for data files (eg {type,levtype} )
#
# Confluence documentation on how to install
# -- dont forget the ~/mars/wind


# fdb5 library

list( APPEND fdb5_srcs
    AdoptVisitor.cc
    AdoptVisitor.h
    ArchiveVisitor.cc
    ArchiveVisitor.h
    Archiver.cc
    Archiver.h
    BTreeIndex.cc
    BTreeIndex.h
    BaseArchiveVisitor.cc
    BaseArchiveVisitor.h
    DB.cc
    DB.h
    Error.cc
    Error.h
    FDBFileHandle.cc
    FDBFileHandle.h
    FileStore.cc
    FileStore.h
    GribArchiver.cc
    GribArchiver.h
    GribDecoder.cc
    GribDecoder.h
    GribIndexer.cc
    GribIndexer.h
    HandleGatherer.cc
    HandleGatherer.h
    Index.cc
    Index.h
    IndexAxis.cc
    IndexAxis.h
    Key.cc
    Key.h
    MasterConfig.cc
    MasterConfig.h
    MatchAlways.cc
    MatchAlways.h
    MatchAny.cc
    MatchAny.h
    MatchHidden.cc
    MatchHidden.h
    MatchOptional.cc
    MatchOptional.h
    MatchValue.cc
    MatchValue.h
    Matcher.cc
    Matcher.h
    NotFound.cc
    NotFound.h
    Predicate.cc
    Predicate.h
    PurgeVisitor.cc
    PurgeVisitor.h
    ReadVisitor.cc
    ReadVisitor.h
    RequestParser.cc
    RequestParser.h
    Retriever.cc
    Retriever.h
    Rule.cc
    Rule.h
    Schema.cc
    Schema.h
    SchemaParser.cc
    SchemaParser.h
    TextIndex.cc
    TextIndex.h
    TocDB.cc
    TocDB.h
    TocDBReader.cc
    TocDBReader.h
    TocDBWriter.cc
    TocDBWriter.h
    TocHandler.cc
    TocHandler.h
    TocAddIndex.cc
    TocAddIndex.h
    TocList.cc
    TocList.h
    TocPrint.cc
    TocPrint.h
    TocRecord.cc
    TocRecord.h
    TocReverseIndexes.cc
    TocReverseIndexes.h
    Type.cc
    Type.h
    TypeAbbreviation.cc
    TypeAbbreviation.h
    TypeClimateDaily.cc
    TypeClimateDaily.h
    TypeClimateMonthly.cc
    TypeClimateMonthly.h
    TypeDate.cc
    TypeDate.h
    TypeDefault.cc
    TypeDefault.h
    TypeDouble.cc
    TypeDouble.cc
    TypeDouble.h
    TypeDouble.h
    TypeGrid.cc
    TypeGrid.h
    TypeIgnore.cc
    TypeIgnore.h
    TypeInteger.cc
    TypeInteger.h
    TypeMonth.cc
    TypeMonth.h
    TypeParam.cc
    TypeParam.h
    TypeStep.cc
    TypeStep.h
    TypesFactory.cc
    TypesFactory.h
    TypesRegistry.cc
    TypesRegistry.h
    WriteVisitor.cc
    WriteVisitor.h
)

ecbuild_add_library(

    TARGET  mars-fdb5

    SOURCES
        ${fdb5_srcs}

    INSTALL_HEADERS LISTED

    HEADER_DESTINATION
        ${INSTALL_INCLUDE_DIR}/fdb5

    PRIVATE_INCLUDES
        ${GRIB_API_INCLUDE_DIRS}

    LIBS
        Mars
        Common
        eckit
        grib_api
)

add_subdirectory( legacy )

list( APPEND fdb5_tools
    fdb5_index
    fdb5_archive
    fdb5_list
    fdb5_extract
    fdb5_retrieve
    fdb5_purge
    fdb5_schema
    )

foreach( _tool ${fdb5_tools} )

  ecbuild_add_executable( TARGET    ${_tool}
                          SOURCES   ${_tool}.cc
                          LIBS      mars-fdb5 eckit_option )

endforeach()
