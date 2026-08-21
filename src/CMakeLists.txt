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

    # Python build tooling is run through uvx rather than the build machine's interpreter, so a
    # CI runner that ships only an interpreter can still build the wheel. uvx resolves each tool
    # into an ephemeral environment; build-requirements.txt lists what has to be in it.
    set(Z3FDB_BUILD_REQUIREMENTS "${CMAKE_CURRENT_SOURCE_DIR}/../python/z3fdb-build-requirements.txt")
    if(NOT EXISTS "${Z3FDB_BUILD_REQUIREMENTS}")
        message(FATAL_ERROR "Missing ${Z3FDB_BUILD_REQUIREMENTS}, required to build the z3fdb wheel.")
    endif()

    # Runtime dependencies of the wheel. setup.py reads this into install_requires, so it is
    # staged next to the generated setup.py below. Kept separate from the build requirements so
    # that build tooling never reaches the wheel metadata.
    set(Z3FDB_RUNTIME_REQUIREMENTS "${CMAKE_CURRENT_SOURCE_DIR}/../python/z3fdb-requirements.txt")
    if(NOT EXISTS "${Z3FDB_RUNTIME_REQUIREMENTS}")
        message(FATAL_ERROR "Missing ${Z3FDB_RUNTIME_REQUIREMENTS}, required to build the z3fdb wheel.")
    endif()

    find_program(UVX_EXECUTABLE NAMES uvx
                 DOC "uv tool runner (uvx), used to run the Python build tooling")
    if(NOT UVX_EXECUTABLE)
        message(FATAL_ERROR
            "uvx not found, but ENABLE_PYTHON_ZARR_INTERFACE=ON needs it to build the wheel. "
            "Install uv (https://docs.astral.sh/uv/) or point -DUVX_EXECUTABLE=/path/to/uvx at it.")
    endif()

    # Create all staging subdirectories at configure time so build-time copy
    # commands do not need to worry about missing parent directories.
    file(MAKE_DIRECTORY
        "${Z3FDB_STAGING}"
        "${Z3FDB_STAGING}/z3fdb"
        "${Z3FDB_STAGING}/z3fdb/_internal"
        "${Z3FDB_STAGING}/pychunked_data_view"
        "${Z3FDB_STAGING}/chunked_data_view_bindings"
        "${Z3FDB_STAGING}/chunked_data_view_bindings-stubs"
    )

    # ---- Per-file copy commands (tracked: re-copy when source changes) ----
    # Each add_custom_command pairs one source file with its staging destination.
    # cmake re-runs the copy whenever the source is newer than the destination.
    set(_z3fdb_py_sources
        z3fdb/__init__.py
        z3fdb/simple_store_builder.py
        z3fdb/custom_store_builder.py
        z3fdb/z3fdb_error.py
        z3fdb/_internal/__init__.py
        z3fdb/_internal/zarr.py
        pychunked_data_view/__init__.py
        pychunked_data_view/chunked_data_view.py
        pychunked_data_view/exceptions.py
        chunked_data_view_bindings/__init__.py
    )

    # A module that exists in src/ but is missing from the list above is silently absent from
    # the staging tree and the wheel: nothing fails, the import just goes missing at runtime.
    # Fail at configure time instead.
    file(GLOB_RECURSE _z3fdb_py_found
         RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
         "z3fdb/*.py"
         "pychunked_data_view/*.py"
         "chunked_data_view_bindings/*.py")
    foreach(_found IN LISTS _z3fdb_py_found)
        if(NOT "${_found}" IN_LIST _z3fdb_py_sources)
            message(FATAL_ERROR
                "${_found} is not listed in _z3fdb_py_sources (src/CMakeLists.txt), so it "
                "would never reach the z3fdb staging tree or the wheel. Add it there.")
        endif()
    endforeach()

    set(_z3fdb_staged_py_files)
    foreach(_rel IN LISTS _z3fdb_py_sources)
        set(_src "${CMAKE_CURRENT_SOURCE_DIR}/${_rel}")
        set(_dst "${Z3FDB_STAGING}/${_rel}")
        add_custom_command(
            OUTPUT  "${_dst}"
            COMMAND ${CMAKE_COMMAND} -E copy "${_src}" "${_dst}"
            DEPENDS "${_src}"
            COMMENT "Copying ${_rel} to staging..."
        )
        list(APPEND _z3fdb_staged_py_files "${_dst}")
    endforeach()

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

    # setup.py reads install_requires from a file next to itself, so stage a copy.
    add_custom_command(
        OUTPUT ${Z3FDB_STAGING}/z3fdb-requirements.txt
        COMMAND ${CMAKE_COMMAND} -E copy
                ${Z3FDB_RUNTIME_REQUIREMENTS}
                ${Z3FDB_STAGING}/z3fdb-requirements.txt
        DEPENDS ${Z3FDB_RUNTIME_REQUIREMENTS}
        COMMENT "Copying z3fdb-requirements.txt to staging..."
    )
    add_subdirectory(chunked_data_view_bindings)

    # ---- Stub generation ----
    # chunked_data_view_bindings/__init__.py calls findlibs.load("fdb5") before
    # the extension module is imported. FDB5_HOME points findlibs directly at the
    # fdb5 build output directory so it can find fdb5lib there.
    #
    # Stubs are written to a PEP 561 stub-only package directory
    # (chunked_data_view_bindings-stubs/) in the staging tree. This keeps the
    # runtime package (chunked_data_view_bindings/) free of .pyi files and lets
    # type checkers discover stubs via the standard stub-package lookup.
    add_custom_command(
        OUTPUT ${CMAKE_BINARY_DIR}/z3fdb.stubs.stamp
        COMMAND ${CMAKE_COMMAND} -E env
                "PYTHONPATH=${Z3FDB_STAGING}"
                "FDB5_HOME=${CMAKE_BINARY_DIR}"
                ${UVX_EXECUTABLE}
                    --python ${Python_VERSION}
                    --from pybind11-stubgen
                    --with-requirements ${Z3FDB_BUILD_REQUIREMENTS}
                    --isolated
                    pybind11-stubgen
                        chunked_data_view_bindings
                        -o ${CMAKE_BINARY_DIR}/stubs_tmp
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_BINARY_DIR}/stubs_tmp/chunked_data_view_bindings/chunked_data_view_bindings.pyi
                ${Z3FDB_STAGING}/chunked_data_view_bindings-stubs/chunked_data_view_bindings.pyi
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_BINARY_DIR}/stubs_tmp/chunked_data_view_bindings/__init__.pyi
                ${Z3FDB_STAGING}/chunked_data_view_bindings-stubs/__init__.pyi
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/stubs_tmp
        COMMAND ${CMAKE_COMMAND} -E touch
                ${Z3FDB_STAGING}/chunked_data_view_bindings-stubs/py.typed
        COMMAND ${CMAKE_COMMAND} -E touch z3fdb.stubs.stamp
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        DEPENDS
            ${Z3FDB_STAGING}/chunked_data_view_bindings/__init__.py
            chunked_data_view_bindings
            fdb5
        COMMENT "Generating stubs for chunked_data_view_bindings (-> chunked_data_view_bindings-stubs/)..."
    )

    # ---- Wheel ----
    add_custom_command(
        OUTPUT ${CMAKE_BINARY_DIR}/wheel.stamp
        COMMAND ${UVX_EXECUTABLE}
                    --python ${Python_VERSION}
                    --from build
                    --isolated
                    --with-requirements ${Z3FDB_BUILD_REQUIREMENTS}
                    pyproject-build --wheel ${Z3FDB_STAGING} -o .
        COMMAND ${CMAKE_COMMAND} -E touch wheel.stamp
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        DEPENDS
            ${_z3fdb_staged_py_files}
            ${Z3FDB_STAGING}/README.md
            ${Z3FDB_STAGING}/LICENSE
            ${Z3FDB_STAGING}/z3fdb-requirements.txt
            ${CMAKE_BINARY_DIR}/z3fdb.stubs.stamp
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
