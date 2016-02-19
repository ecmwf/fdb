# fdb5 library

list( APPEND fdb5_srcs
    Archiver.h
    Archiver.cc
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
