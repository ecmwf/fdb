############################################################################################
# libraries

ecbuild_find_project_files()

add_subdirectory(fdb5)
add_subdirectory(chunked_data_view)

if (HAVE_PYTHON_ZARR_INTERFACE)
    # We create the complete python package layout at this location.
    # This allows us to run python wheel creation at this path and 
    # to put this path on the PYTHONPATH to allow direct use of
    # z3fdb, e.g. for testing or local exploration.
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
        @ONLY
    )
    add_subdirectory(chunked_data_view_bindings)
    set(_z3fdb_package_files
        "z3fdb/simple_store_builder.py"
        "z3fdb/z3fdb_error.py"
        "z3fdb/__init__.py"
        "z3fdb/_internal/zarr.py"
        "pychunked_data_view/__init__.py"
        "pychunked_data_view/chunked_data_view.py"
        "pychunked_data_view/exceptions.py"
    )
    add_custom_command(
        OUTPUT ${CMAKE_BINARY_DIR}/wheel.stamp
        COMMAND ${Python_INTERPRETER} -m build --wheel ${Z3FDB_STAGING} -o .
        COMMAND ${CMAKE_COMMAND} -E touch wheel.stamp
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        DEPENDS 
            ${_z3fdb_package_files}
            chunked_data_view_bindings
        COMMENT "Building Python wheel for z3fdb..."
    )
    add_custom_target(z3fdb-wheel ALL DEPENDS ${CMAKE_BINARY_DIR}/wheel.stamp)
endif()

if (HAVE_DUMMY_DAOS)
	add_subdirectory(dummy_daos)
endif()
