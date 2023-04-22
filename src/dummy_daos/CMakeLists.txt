ecbuild_add_library(

    TARGET daos

    INSTALL_HEADERS_LIST
        daos.h

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

if(HAVE_DAOS_ADMIN)

    ecbuild_add_library(

        TARGET daos_tests

        INSTALL_HEADERS LISTED

        HEADER_DESTINATION ${INSTALL_INCLUDE_DIR}

        SOURCES
            daos/tests_lib.h
            daos/tests_lib.cc
        
        PRIVATE_INCLUDES
            ${PROJECT_SOURCE_DIR}/../daos.h
            ${PROJECT_SOURCE_DIR}/../dummy_daos.h

        PRIVATE_LIBS
            daos

    )
    
endif()
