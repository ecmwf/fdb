# fdb5-legacy library

list( APPEND fdb5_legacy_srcs
  FDBScanner.h
  FDBScanner.cc
  FDBIndexScanner.h
  FDBIndexScanner.cc
#  IndexCache.h
#  IndexCache.cc
)

ecbuild_add_library(

    TARGET  mars-fdb5-legacy

    SOURCES
        ${fdb5_legacy_srcs}

    INSTALL_HEADERS LISTED

    HEADER_DESTINATION
        ${INSTALL_INCLUDE_DIR}/fdb5/legacy

    PRIVATE_INCLUDES
        ${GRIB_API_INCLUDE_DIRS}

    LIBS
        pointdb mars-fdb5
)

list( APPEND fdb5_legacy_tools
    fdb5_adopt
    )

foreach( _tool ${fdb5_legacy_tools} )

  ecbuild_add_executable( TARGET    ${_tool}
                          SOURCES   ${_tool}.cc
                          LIBS      mars-fdb5-legacy  )

endforeach()
