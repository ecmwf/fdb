ecbuild_add_library(

    TARGET  daos

    INSTALL_HEADERS LISTED

    HEADER_DESTINATION ${INSTALL_INCLUDE_DIR}

    SOURCES
        daos.h
        daos.cc
        dummy_daos.h
        dummy_daos.cc

    PUBLIC_LIBS
        eckit
        uuid

)