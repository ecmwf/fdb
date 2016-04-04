# fdb5 library

list( APPEND fdb5_srcs

    Archiver.h
    Archiver.cc

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
        ${MARSKIT_INCLUDE_DIRS}
        ${GRIB_API_INCLUDE_DIRS}

    LIBS
        Mars
        Common
        ${MARSKIT_LIBRARIES}
        eckit
        grib_api
)
