ecbuild_add_library(

    TARGET daos TYPE STATIC

    INSTALL_HEADERS LISTED

    HEADER_DESTINATION ${INSTALL_INCLUDE_DIR}

    SOURCES
        daos.h
        daos.cc
        dummy_daos.h
        dummy_daos.cc
        daos/tests_lib.h
        daos/tests_lib.cc

    PUBLIC_LIBS
        eckit
        uuid

)
