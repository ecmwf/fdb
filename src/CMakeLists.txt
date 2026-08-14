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
    # Copy README.md and LICENSE at build time so changes are picked up
    # without needing to re-run cmake configure
    add_custom_command(
        OUTPUT ${Z3FDB_STAGING}/README.md
        COMMAND ${CMAKE_COMMAND} -E copy
            "${CMAKE_CURRENT_SOURCE_DIR}/z3fdb/README.md"
            "${Z3FDB_STAGING}/README.md"
        DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/z3fdb/README.md"
        COMMENT "Copying z3fdb README.md to staging..."
    )
    add_custom_command(
        OUTPUT ${Z3FDB_STAGING}/LICENSE
        COMMAND ${CMAKE_COMMAND} -E copy
            "${CMAKE_CURRENT_SOURCE_DIR}/../LICENSE"
            "${Z3FDB_STAGING}/LICENSE"
        DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/../LICENSE"
        COMMENT "Copying LICENSE to staging..."
    )
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/z3fdb_setup.py.in
        ${Z3FDB_STAGING}/setup.py
        @ONLY
    )
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/z3fdb_setup.cfg.in
        ${Z3FDB_STAGING}/setup.cfg
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
        "chunked_data_view_bindings/__init__.py"
    )
    list(APPEND _z3fdb_package_files
        "${Z3FDB_STAGING}/README.md"
        "${Z3FDB_STAGING}/LICENSE"
    )
    # chunked_data_view_bindings/__init__.py calls findlibs.load("fdb5") before
    # the extension module is imported, and findlibs to locate the library.
    # FDB5_HOME points findlibs directly at the fdb5 build output directory so
    # it can find fdb5lib there.
    #
    # Stubgen writes into a temporary directory; only chunked_data_view_bindings.pyi
    # is copied next to the *.so. The temp dir (including any __init__.pyi) is
    # removed afterwards.
    # TODO(TKR): Enable this once the isolated build environment for python wheels is in place
    # add_custom_command(
    #     OUTPUT ${CMAKE_BINARY_DIR}/stubs.stamp
    #     COMMAND ${CMAKE_COMMAND} -E env
    #             "PYTHONPATH=${Z3FDB_STAGING}"
    #             "FDB5_HOME=${CMAKE_BINARY_DIR}"
    #             ${Python_EXECUTABLE} -m pybind11_stubgen
    #                 chunked_data_view_bindings.chunked_data_view_bindings
    #                 -o ${CMAKE_BINARY_DIR}/stubs_tmp
    #     COMMAND ${CMAKE_COMMAND} -E copy
    #             ${CMAKE_BINARY_DIR}/stubs_tmp/chunked_data_view_bindings/chunked_data_view_bindings.pyi
    #             ${Z3FDB_STAGING}/chunked_data_view_bindings/chunked_data_view_bindings.pyi
    #     COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/stubs_tmp
    #     COMMAND ${CMAKE_COMMAND} -E touch
    #             ${Z3FDB_STAGING}/chunked_data_view_bindings/py.typed
    #     COMMAND ${CMAKE_COMMAND} -E touch stubs.stamp
    #     WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    #     DEPENDS
    #         "chunked_data_view_bindings/__init__.py"
    #         chunked_data_view_bindings
    #         fdb5
    #     COMMENT "Generating stubs and py.typed for chunked_data_view_bindings..."
    # )
    # list(APPEND _z3fdb_package_files
    #     ${CMAKE_BINARY_DIR}/stubs.stamp
    # )

    add_custom_command(
        OUTPUT ${CMAKE_BINARY_DIR}/wheel.stamp
        COMMAND ${Python_EXECUTABLE} -m build --wheel ${Z3FDB_STAGING} -o .
        COMMAND ${CMAKE_COMMAND} -E touch wheel.stamp
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        DEPENDS
            ${_z3fdb_package_files}
            chunked_data_view_bindings
        COMMENT "Building Python wheel for z3fdb..."
    )
    add_custom_target(z3fdb-wheel ALL DEPENDS ${CMAKE_BINARY_DIR}/wheel.stamp)
endif()

if (HAVE_PYTHON_FDB_INTERFACE)
    # We create the complete python package layout at this location.
    # This allows us to run python wheel creation at this path and 
    # to put this path on the PYTHONPATH to allow direct use of
    # pyfdb, e.g. for testing or local exploration.
    set(PYFDB_STAGING "${CMAKE_BINARY_DIR}/pyfdb-python-package-staging")
    file(MAKE_DIRECTORY "${PYFDB_STAGING}")
    file(CREATE_LINK
        "${CMAKE_CURRENT_SOURCE_DIR}/pyfdb"
        "${PYFDB_STAGING}/pyfdb" SYMBOLIC
    )
    # Copy README.md and LICENSE at build time so changes are picked up
    # without needing to re-run cmake configure
    add_custom_command(
        OUTPUT ${PYFDB_STAGING}/README.md
        COMMAND ${CMAKE_COMMAND} -E copy
            "${CMAKE_CURRENT_SOURCE_DIR}/pyfdb/README.md"
            "${PYFDB_STAGING}/README.md"
        DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/pyfdb/README.md"
        COMMENT "Copying pyfdb README.md to staging..."
    )
    add_custom_command(
        OUTPUT ${PYFDB_STAGING}/LICENSE
        COMMAND ${CMAKE_COMMAND} -E copy
            "${CMAKE_CURRENT_SOURCE_DIR}/../LICENSE"
            "${PYFDB_STAGING}/LICENSE"
        DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/../LICENSE"
        COMMENT "Copying LICENSE to staging..."
    )
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/pyfdb_setup.py.in
        ${PYFDB_STAGING}/setup.py
        @ONLY
    )
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/pyfdb_setup.cfg.in
        ${PYFDB_STAGING}/setup.cfg
        @ONLY
    )
    add_subdirectory(pyfdb_bindings)
    file(GLOB_RECURSE
        _pyfdb_package_files
        "pyfdb/*.py"
        "pyfdb/_internal/*.py"
    )
    list(APPEND _pyfdb_package_files
        "${CMAKE_CURRENT_SOURCE_DIR}/pyfdb/README.md"
        "${CMAKE_CURRENT_SOURCE_DIR}/../LICENSE"
    )
    add_custom_command(
        OUTPUT ${CMAKE_BINARY_DIR}/pyfdb.wheel.stamp
        COMMAND ${Python_EXECUTABLE} -m build --wheel ${PYFDB_STAGING} -o .
        COMMAND ${CMAKE_COMMAND} -E touch pyfdb.wheel.stamp
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        DEPENDS ${_pyfdb_package_files} pyfdb_bindings
                ${PYFDB_STAGING}/README.md ${PYFDB_STAGING}/LICENSE
        COMMENT "Building Python wheel for pyfdb..."
    )
    add_custom_target(pyfdb-wheel ALL DEPENDS ${CMAKE_BINARY_DIR}/pyfdb.wheel.stamp)
endif()


if (HAVE_DUMMY_DAOS)
	add_subdirectory(dummy_daos)
endif()
