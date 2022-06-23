ecbuild_add_library(

    TARGET  daos

    SOURCES
        daos.h
        daos.cc
        dummy_daos.h
        dummy_daos.cc

    PUBLIC_LIBS
        eckit

)