# fdb5 library

list( APPEND fdb5_srcs
    Archiver.h
    Archiver.cc
    FdbTask.h
    FdbTask.cc
    Winds.h
    Winds.cc
    Retriever.h
    Retriever.cc
    NotFound.h
    NotFound.cc
    Op.h
    Op.cc
    RetrieveOp.h
    RetrieveOp.cc
    ForwardOp.h
    ForwardOp.cc
    UVOp.h
    UVOp.cc
    TocDB.h
    TocDB.cc
    DB.h
    DB.cc
    MasterConfig.h
    MasterConfig.cc
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
