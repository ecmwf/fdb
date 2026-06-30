find_package(Doxygen 1.9 REQUIRED)
find_package(Sphinx REQUIRED)

# --- Paths ---
set(FDB_API_HEADER_DIR "${PROJECT_SOURCE_DIR}/src/fdb5/api")
set(DOXYGEN_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/doxygen")
set(SPHINX_OUTPUT_DIR  "${CMAKE_CURRENT_BINARY_DIR}/sphinx")
set(DOXYGEN_XML_DIR    "${DOXYGEN_OUTPUT_DIR}/xml")
set(DOXYGEN_INPUT_DIR  "${FDB_API_HEADER_DIR}")
set(FDB_VERSION        "${PROJECT_VERSION}")

# --- Input file globs ---
file(GLOB_RECURSE FDB_PUBLIC_HEADERS CONFIGURE_DEPENDS "${FDB_API_HEADER_DIR}/*.h")
file(GLOB_RECURSE PYFDB_SRCS CONFIGURE_DEPENDS "${PROJECT_SOURCE_DIR}/src/pyfdb/*.py")
file(GLOB_RECURSE Z3FDB_SRCS CONFIGURE_DEPENDS "${PROJECT_SOURCE_DIR}/src/z3fdb/*.py")
file(GLOB_RECURSE SPHINX_INPUT_FILES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.rst")
list(FILTER SPHINX_INPUT_FILES EXCLUDE REGEX "_build")

# --- Output stamps ---
set(DOXYGEN_INDEX_FILE "${DOXYGEN_OUTPUT_DIR}/html/index.html")
set(SPHINX_INDEX_FILE  "${SPHINX_OUTPUT_DIR}/index.html")

# --- Configure Doxyfile ---
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in"
    "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile"
    @ONLY
)

# --- Doxygen ---
add_custom_command(
    OUTPUT  ${DOXYGEN_INDEX_FILE}
    COMMAND ${CMAKE_COMMAND} -E make_directory "${DOXYGEN_OUTPUT_DIR}"
    COMMAND ${DOXYGEN_EXECUTABLE} "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile"
    DEPENDS ${FDB_PUBLIC_HEADERS} "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile"
    MAIN_DEPENDENCY "${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Running Doxygen on FDB API headers"
)

# --- Sphinx ---
add_custom_command(
    OUTPUT  ${SPHINX_INDEX_FILE}
    COMMAND
        ${SPHINX_EXECUTABLE} -j auto -E -a -T
        -Dbreathe_projects.FDB=${DOXYGEN_XML_DIR}
        -Dversion=${FDB_VERSION}
        -Drelease=${FDB_VERSION}
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${SPHINX_OUTPUT_DIR}"
    DEPENDS ${DOXYGEN_INDEX_FILE} ${SPHINX_INPUT_FILES}
            ${PYFDB_SRCS} ${Z3FDB_SRCS}
            "${CMAKE_CURRENT_SOURCE_DIR}/conf.py"
    COMMENT "Running Sphinx to build HTML documentation"
)

# --- Target ---
add_custom_target(fdb-doc ALL DEPENDS ${SPHINX_INDEX_FILE} ${DOXYGEN_INDEX_FILE})
