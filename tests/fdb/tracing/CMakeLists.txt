ecbuild_add_test( TARGET  fdb_test_tracing
                  SOURCES test_tracing.cc
                  LIBS    fdb5 )

if( HAVE_FDB_BUILD_TOOLS )
    configure_file( fdb_tracing_tools.sh.in fdb_tracing_tools.sh @ONLY )

    ecbuild_add_test( TARGET      fdb_test_tracing_tools
                      TYPE        SCRIPT
                      COMMAND     fdb_tracing_tools.sh
                      ENVIRONMENT "${test_environment}" )
endif()
