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
    Archiver.cc
    Archiver.h
    ArchiveVisitor.cc
    ArchiveVisitor.h
    toc/BTreeIndex.h
    toc/BTreeIndex.cc
    BaseArchiveVisitor.cc
    BaseArchiveVisitor.h
    DB.cc
    DB.h
    FDBFileHandle.cc
    FDBFileHandle.h
    FDBTool.cc
    FDBTool.h
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
    rule/MatchAlways.cc
    rule/MatchAlways.h
    rule/MatchAny.cc
    rule/MatchAny.h
    rule/Matcher.cc
    rule/Matcher.h
    rule/MatchHidden.cc
    rule/MatchHidden.h
    rule/MatchOptional.cc
    rule/MatchOptional.h
    rule/MatchValue.cc
    rule/MatchValue.h
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
    rule/Rule.cc
    rule/Rule.h
    rule/Schema.cc
    rule/Schema.h
    rule/SchemaParser.cc
    rule/SchemaParser.h
    toc/TocDB.cc
    toc/TocDB.h
    toc/TocDBReader.cc
    toc/TocDBReader.h
    toc/TocDBWriter.cc
    toc/TocDBWriter.h
    toc/TocHandler.cc
    toc/TocHandler.h
    toc/TocIndex.cc
    toc/TocIndex.h
    toc/TocRecord.cc
    toc/TocRecord.h
    type/Type.cc
    type/Type.h
    type/TypeAbbreviation.cc
    type/TypeAbbreviation.h
    type/TypeClimateDaily.cc
    type/TypeClimateDaily.h
    type/TypeClimateMonthly.cc
    type/TypeClimateMonthly.h
    type/TypeDate.cc
    type/TypeDate.h
    type/TypeDefault.cc
    type/TypeDefault.h
    type/TypeDouble.cc
    type/TypeDouble.cc
    type/TypeDouble.h
    type/TypeDouble.h
    type/TypeGrid.cc
    type/TypeGrid.h
    type/TypeIgnore.cc
    type/TypeIgnore.h
    type/TypeInteger.cc
    type/TypeInteger.h
    type/TypeMonth.cc
    type/TypeMonth.h
    type/TypeParam.cc
    type/TypeParam.h
    type/TypesFactory.cc
    type/TypesFactory.h
    type/TypesRegistry.cc
    type/TypesRegistry.h
    type/TypeStep.cc
    type/TypeStep.h
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
        eckit_option
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
                          LIBS
                          mars-fdb5 )

endforeach()

