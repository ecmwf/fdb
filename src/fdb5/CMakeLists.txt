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
    config/MasterConfig.cc
    config/MasterConfig.h
    database/Archiver.cc
    database/Archiver.h
    database/ArchiveVisitor.cc
    database/ArchiveVisitor.h
    database/BaseArchiveVisitor.cc
    database/BaseArchiveVisitor.h
    database/DB.cc
    database/DB.h
    database/FileStore.cc
    database/FileStore.h
    database/Index.cc
    database/Index.h
    database/IndexAxis.cc
    database/IndexAxis.h
    database/Key.cc
    database/Key.h
    database/ReadVisitor.cc
    database/ReadVisitor.h
    database/Retriever.cc
    database/Retriever.h
    database/WriteVisitor.cc
    database/WriteVisitor.h
    grib/GribArchiver.cc
    grib/GribArchiver.h
    grib/GribDecoder.cc
    grib/GribDecoder.h
    grib/GribIndexer.cc
    grib/GribIndexer.h
    io/FDBFileHandle.cc
    io/FDBFileHandle.h
    io/HandleGatherer.cc
    io/HandleGatherer.h
    rules/MatchAlways.cc
    rules/MatchAlways.h
    rules/MatchAny.cc
    rules/MatchAny.h
    rules/Matcher.cc
    rules/Matcher.h
    rules/MatchHidden.cc
    rules/MatchHidden.h
    rules/MatchOptional.cc
    rules/MatchOptional.h
    rules/MatchValue.cc
    rules/MatchValue.h
    rules/Predicate.cc
    rules/Predicate.h
    rules/Rule.cc
    rules/Rule.h
    rules/Schema.cc
    rules/Schema.h
    rules/SchemaParser.cc
    rules/SchemaParser.h
    toc/AdoptVisitor.cc
    toc/AdoptVisitor.h
    toc/BTreeIndex.cc
    toc/BTreeIndex.h
    toc/PurgeVisitor.cc
    toc/PurgeVisitor.h
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
    tools/FDBTool.cc
    tools/FDBTool.h
    tools/RequestParser.cc
    tools/RequestParser.h
    types/Type.cc
    types/Type.h
    types/TypeAbbreviation.cc
    types/TypeAbbreviation.h
    types/TypeClimateDaily.cc
    types/TypeClimateDaily.h
    types/TypeClimateMonthly.cc
    types/TypeClimateMonthly.h
    types/TypeDate.cc
    types/TypeDate.h
    types/TypeDefault.cc
    types/TypeDefault.h
    types/TypeDouble.cc
    types/TypeDouble.cc
    types/TypeDouble.h
    types/TypeDouble.h
    types/TypeGrid.cc
    types/TypeGrid.h
    types/TypeIgnore.cc
    types/TypeIgnore.h
    types/TypeInteger.cc
    types/TypeInteger.h
    types/TypeMonth.cc
    types/TypeMonth.h
    types/TypeParam.cc
    types/TypeParam.h
    types/TypesFactory.cc
    types/TypesFactory.h
    types/TypesRegistry.cc
    types/TypesRegistry.h
    types/TypeStep.cc
    types/TypeStep.h
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
    fdb5_where
    )

foreach( _tool ${fdb5_tools} )

  ecbuild_add_executable( TARGET    ${_tool}
                          SOURCES   tools/${_tool}.cc
                          LIBS
                          mars-fdb5 )

endforeach()

