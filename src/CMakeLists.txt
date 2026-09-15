############################################################################################
# libraries

ecbuild_find_project_files()

add_subdirectory(fdb5)

if (HAVE_PYTHON_FDB_INTERFACE)
    # We create the complete python package layout at this location.
    # This allows us to run python wheel creation at this path and 
    # to put this path on the PYTHONPATH to allow direct use of
    # pyfdb, e.g. for testing or local exploration.
    set(PYFDB_STAGING "${CMAKE_BINARY_DIR}/pyfdb-python-package-staging")
    file(MAKE_DIRECTORY "${PYFDB_STAGING}")

    # Granular symlinks so the compiled .so lands in staging only,
    # not in src/pyfdb/bindings/. Source edits are still reflected immediately.
    file(MAKE_DIRECTORY "${PYFDB_STAGING}/pyfdb")
    file(GLOB _pyfdb_toplevel_py "${CMAKE_CURRENT_SOURCE_DIR}/pyfdb/*.py")
    foreach(_py ${_pyfdb_toplevel_py})
        get_filename_component(_name ${_py} NAME)
        file(CREATE_LINK "${_py}" "${PYFDB_STAGING}/pyfdb/${_name}" SYMBOLIC)
    endforeach()
    file(CREATE_LINK
        "${CMAKE_CURRENT_SOURCE_DIR}/pyfdb/_internal"
        "${PYFDB_STAGING}/pyfdb/_internal" SYMBOLIC
    )
    file(MAKE_DIRECTORY "${PYFDB_STAGING}/pyfdb/bindings")
    file(CREATE_LINK
        "${CMAKE_CURRENT_SOURCE_DIR}/pyfdb/bindings/__init__.py"
        "${PYFDB_STAGING}/pyfdb/bindings/__init__.py" SYMBOLIC
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
    add_subdirectory(pyfdb/bindings)
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
