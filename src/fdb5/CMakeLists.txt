### TODO
#
#  * fdbRoot is a list of paths to visit ---> speak to Hodge
#  * check overwrite
#  * on archive consume all keys (from GRIB mars keys)
#
#  * fdb_ingest --path fdbroot
#      - check for duplicates
#      - check that keysIdx are fully defined
#      - build a suite
#
#  * check fdbRoot depending on request
#
#  * client "database" for direct fdb5 read
#
#  * copy the schema (fdbRules) into toc dir and read it fromt here on retrieves
#
#  * fdb5_ls -class od -expver 0001
#
#  * multihandle
#    - derive from MultiHandle
#    - override compress() to compress & sort children
#
#  * consider using more than a service node on the HPC


# fdb5 library

list( APPEND fdb5_srcs

    Archiver.h
    Archiver.cc
    GribArchiver.h
    GribArchiver.cc

    Retriever.h
    Retriever.cc

    Winds.h
    Winds.cc

    Op.h
    Op.cc
    RetrieveOp.h
    RetrieveOp.cc
    ForwardOp.h
    ForwardOp.cc
    UVOp.h
    UVOp.cc

    MasterConfig.h
    MasterConfig.cc
    Key.h
    Key.cc
    Schema.h
    Schema.cc
    Matcher.h
    Matcher.cc
    MatchAny.h
    MatchAny.cc
    MatchValue.h
    MatchValue.cc
    MatchAlways.h
    MatchAlways.cc
    Predicate.h
    Predicate.cc
    Rule.h
    Rule.cc
    Rules.h
    Rules.cc
    RulesParser.h
    RulesParser.cc
    ReadVisitor.h
    ReadVisitor.cc
    WriteVisitor.h
    WriteVisitor.cc

    Handlers.h
    Handlers.cc
    KeywordType.h
    KeywordType.cc
    KeywordHandler.h
    KeywordHandler.cc
    DefaultKeywordHandler.h
    DefaultKeywordHandler.cc
    ParamHandler.h
    ParamHandler.cc
    IntegerHandler.h
    IntegerHandler.cc

    DB.h
    DB.cc
    TocDB.h
    TocDB.cc
    TocDBReader.h
	TocDBReader.cc
	TocDBWriter.h
	TocDBWriter.cc
    TocSchema.h
    TocSchema.cc

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

ecbuild_add_executable( TARGET    fdb5_insert
                        SOURCES   fdb5_insert.cc
                        LIBS      mars-fdb5 )

ecbuild_add_executable( TARGET    fdb5_extract
                        SOURCES   fdb5_extract.cc
                        LIBS      mars-fdb5 )

ecbuild_add_executable( TARGET    fdb5_rules
                        SOURCES   fdb5_rules.cc
                        LIBS      mars-fdb5 )
