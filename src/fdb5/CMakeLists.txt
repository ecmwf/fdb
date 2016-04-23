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

    Archiver.h
    Archiver.cc
    GribArchiver.h
    GribArchiver.cc
    GribIndexer.h
    GribIndexer.cc
    GribDecoder.h
    GribDecoder.cc

    Retriever.h
    Retriever.cc

    FDBFileHandle.h
    FDBFileHandle.cc

    MasterConfig.h
    MasterConfig.cc
    Key.h
    Key.cc
    Matcher.h
    Matcher.cc
    MatchAny.h
    MatchAny.cc
    MatchValue.h
    MatchValue.cc
    MatchAlways.h
    MatchAlways.cc
    MatchOptional.h
    MatchOptional.cc
    MatchHidden.h
    MatchHidden.cc
    Predicate.h
    Predicate.cc
    Rule.h
    Rule.cc
    Schema.h
    Schema.cc
    SchemaParser.h
    SchemaParser.cc

    ReadVisitor.h
    ReadVisitor.cc
    WriteVisitor.h
    WriteVisitor.cc
    BaseArchiveVisitor.h
    BaseArchiveVisitor.cc
    ArchiveVisitor.h
    ArchiveVisitor.cc
    AdoptVisitor.h
    AdoptVisitor.cc

    TypesRegistry.h
    TypesRegistry.cc
    TypeIgnore.h
    TypeIgnore.cc
    TypeDate.h
    TypeDate.cc
    TypeStep.h
    TypeStep.cc
    TypesFactory.h
    TypesFactory.cc
    Type.h
    Type.cc
    TypeDefault.h
    TypeDefault.cc
    TypeParam.h
    TypeParam.cc
    TypeInteger.h
    TypeInteger.cc
    TypeDouble.h
    TypeDouble.cc
    TypeDouble.h
    TypeDouble.cc
    GridHandler.h
    GridHandler.cc
    TypeMonth.h
    TypeMonth.cc
    TypeClimateDaily.h
    TypeClimateDaily.cc
    TypeClimateMonthly.h
    TypeClimateMonthly.cc
	TypeAbbreviation.h
	TypeAbbreviation.cc

    DB.h
    DB.cc
    TocDB.h
    TocDB.cc
    TocDBReader.h
	TocDBReader.cc
	TocDBWriter.h
	TocDBWriter.cc

    Index.h
    Index.cc
    BTreeIndex.h
    BTreeIndex.cc
    TextIndex.h
    TextIndex.cc
    IndexAxis.h
    IndexAxis.cc

    FileStore.h
    FileStore.cc

    TocActions.h
    TocActions.cc
    TocRecord.h
    TocRecord.cc
    TocHandler.h
    TocHandler.cc

    LegacyKey.h
    LegacyKey.cc

    Error.h
    Error.cc
    NotFound.h
    NotFound.cc

    HandleGatherer.h
    HandleGatherer.cc
    RequestParser.h
    RequestParser.cc
    TypeDateOrMonth.h
    TypeDateOrMonth.cc
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
