if (HAVE_FDB_BUILD_TOOLS) # test scripts use the fdb tools
    add_subdirectory(FDB-238)
    add_subdirectory(FDB-239)
    add_subdirectory(FDB-240)
endif()
