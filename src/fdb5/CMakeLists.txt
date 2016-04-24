### TODO
#
#  * tool to purge
#
#  * run the suite and slowly build the rules
#
#  * fdbRoot is a list of paths to visit ---> speak to Hodge
#  * check overwrite
#
#  * Axis should consider Type
#
#  * fdb_ingest --path fdbroot
#      - check for duplicates
#      - check that keysIdx are fully defined
#      - build a suite
#
#  * client "database" for direct fdb5 read
#
#  * copy the schema (fdbRules) into toc dir and read it fromt here on retrieves
#
#  * fdb5_ls -class od -expver 0001
#
#  * consider using more than a service node on the HPC
#
#  * Rules for data files (eg {type,levtype} )
#
#  * marsadm tools:
#    --  list roots


# fdb5 library

list( APPEND fdb5_srcs
    AdoptVisitor.cc
    AdoptVisitor.h
    Archiver.cc
    Archiver.h
    ArchiveVisitor.cc
    ArchiveVisitor.h
    BaseArchiveVisitor.cc
    BaseArchiveVisitor.h
    BTreeIndex.cc
    BTreeIndex.h
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
    LegacyKey.cc
    LegacyKey.h
    MasterConfig.cc
    MasterConfig.h
    MatchAlways.cc
    MatchAlways.h
    MatchAny.cc
    MatchAny.h
    Matcher.cc
    Matcher.h
    MatchHidden.cc
    MatchHidden.h
    MatchOptional.cc
    MatchOptional.h
    MatchValue.cc
    MatchValue.h
    NotFound.cc
    NotFound.h
    Predicate.cc
    Predicate.h
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
    TocActions.cc
    TocActions.h
    TocDB.cc
    TocDB.h
    TocDBReader.cc
    TocDBReader.h
    TocDBWriter.cc
    TocDBWriter.h
    TocHandler.cc
    TocHandler.h
    TocRecord.cc
    TocRecord.h
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
    TypeDateOrMonth.cc
    TypeDateOrMonth.h
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
    TypesFactory.cc
    TypesFactory.h
    TypesRegistry.cc
    TypesRegistry.h
    TypeStep.cc
    TypeStep.h
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

list( APPEND fdb5_tools
    fdb5_index
    fdb5_insert
    fdb5_extract
    fdb5_retrieve
    fdb5_schema
    )

foreach( _tool ${fdb5_tools} )

  ecbuild_add_executable( TARGET    ${_tool}
                          SOURCES   ${_tool}.cc
                          LIBS      mars-fdb5 )

endforeach()
