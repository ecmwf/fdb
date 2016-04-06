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

    KeywordHandler.h
    KeywordHandler.cc
    DefaultKeywordHandler.h
    DefaultKeywordHandler.cc
    ParamKeywordHandler.h
    ParamKeywordHandler.cc

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

    FileStore.h
    FileStore.cc

    TocActions.h
    TocActions.cc
    TocRecord.h
    TocRecord.cc
    TocHandler.h
    TocHandler.cc

	LegacyTranslator.h
	LegacyTranslator.cc

    Error.h
    Error.cc
    NotFound.h
    NotFound.cc
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
                        INCLUDES  ${GRIB_API_INCLUDE_DIRS}
                        SOURCES   fdb5_insert.cc
                        LIBS      mars-fdb5 )

ecbuild_add_executable( TARGET    fdb5_extract
                        INCLUDES  ${GRIB_API_INCLUDE_DIRS}
                        SOURCES   fdb5_extract.cc
                        LIBS      mars-fdb5 )
