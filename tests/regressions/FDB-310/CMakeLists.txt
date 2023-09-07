ecbuild_add_executable( TARGET    fdb-url
                          CONDITION HAVE_FDB_BUILD_TOOLS
                          SOURCES   fdb-url.cc
                          INCLUDES  ${ECCODES_INCLUDE_DIRS}  # Please don't remove me, I am needed
                          LIBS      fdb5 )

ecbuild_configure_file( FDB-310.sh.in FDB-310.sh @ONLY )
ecbuild_add_test(
    TYPE     SCRIPT
    COMMAND  FDB-310.sh )
