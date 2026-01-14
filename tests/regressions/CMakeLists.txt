if (HAVE_FDB_BUILD_TOOLS) # test scripts use the fdb tools

    if (HAVE_GRIB)
        add_subdirectory(FDB-238)
        add_subdirectory(FDB-239)
        add_subdirectory(FDB-240)
        add_subdirectory(FDB-243)
        add_subdirectory(FDB-245)
        add_subdirectory(FDB-251)
        add_subdirectory(FDB-260)
        add_subdirectory(FDB-264)
        add_subdirectory(FDB-266)
        add_subdirectory(FDB-267)
        add_subdirectory(FDB-268)
        add_subdirectory(FDB-271)
        add_subdirectory(FDB-275)
        add_subdirectory(FDB-276)
        add_subdirectory(FDB-282)
        add_subdirectory(FDB-291)
        add_subdirectory(FDB-292)
        add_subdirectory(FDB-303)
        add_subdirectory(FDB-307)
        add_subdirectory(FDB-310)
        add_subdirectory(FDB-332)
        add_subdirectory(FDB-533)
        add_subdirectory(FDB-535)
        add_subdirectory(FDB-541)
    endif()

    add_subdirectory(FDB-241)

    if (HAVE_FDB_REMOTE)
        add_subdirectory(FDB-419)
        
        if (HAVE_GRIB)
            add_subdirectory(FDB-610)
            add_subdirectory(FDB-258)
            add_subdirectory(FDB-491)
            add_subdirectory(FDB-595)
        endif()

    endif()

endif()

# Tests not using the fdb tools
add_subdirectory(FDB-425)
