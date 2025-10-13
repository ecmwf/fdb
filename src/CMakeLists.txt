############################################################################################
# libraries

ecbuild_find_project_files()

add_subdirectory(fdb5)
add_subdirectory(chunked_data_view)

if (HAVE_PYTHON_ZARR_INTERFACE)
    set(Z3FDB_STAGING "${CMAKE_BINARY_DIR}/z3fdb-python-package-staging")
    file(MAKE_DIRECTORY "${Z3FDB_STAGING}")
    file(CREATE_LINK
        "${CMAKE_CURRENT_SOURCE_DIR}/z3fdb"
        "${Z3FDB_STAGING}/z3fdb" SYMBOLIC
    )
    file(CREATE_LINK
        "${CMAKE_CURRENT_SOURCE_DIR}/pychunked_data_view"
        "${Z3FDB_STAGING}/pychunked_data_view" SYMBOLIC
    )
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/z3fdb_setup.py.in
        ${Z3FDB_STAGING}/setup.py
    )
    add_subdirectory(chunked_data_view_bindings)
endif()

if (HAVE_DUMMY_DAOS)
	add_subdirectory(dummy_daos)
endif()

############################################################################################
# Following directories are not packaged
# currently this is developer only, hence not distributed

if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/sandbox/CMakeLists.txt)
	add_subdirectory(sandbox)
endif()
