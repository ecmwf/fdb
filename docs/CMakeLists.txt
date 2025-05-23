find_package(Doxygen 1.9 REQUIRED)
find_package(Sphinx REQUIRED)

file(GLOB_RECURSE FDB_PUBLIC_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/../src/fdb5/api/*.h)

set(DOXYGEN_INPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../src/fdb5/api)
set(DOXYGEN_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/doxygen)
set(DOXYGEN_INDEX_FILE ${DOXYGEN_OUTPUT_DIR}/html/index.html)
set(DOXYFILE_IN ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in)
set(DOXYFILE_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)

configure_file(${DOXYFILE_IN} ${DOXYFILE_OUT} @ONLY)

add_custom_command(
    OUTPUT ${DOXYGEN_INDEX_FILE}
    DEPENDS ${FDB_PUBLIC_HEADERS}
    COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYFILE_OUT}
    MAIN_DEPENDENCY ${DOXYFILE_OUT} ${DOXYFILE_IN}
    COMMENT "Extracting documentation with doxygen")

add_custom_target(fdb-doxygen ALL DEPENDS ${DOXYGEN_INDEX_FILE})

set(SPHINX_SOURCE ${CMAKE_CURRENT_SOURCE_DIR})
set(SPHINX_BUILD ${CMAKE_CURRENT_BINARY_DIR}/sphinx)
set(SPHINX_INDEX_FILE ${SPHINX_BUILD}/index.html)

file(GLOB_RECURSE SPHINX_INPUT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.rst)

add_custom_command(
    OUTPUT ${SPHINX_INDEX_FILE}
    COMMAND
        ${SPHINX_EXECUTABLE} -b html -j auto
        -Dbreathe_projects.FDB=${DOXYGEN_OUTPUT_DIR}/xml
        -Dversion=${fdb5_VERSION_STR}
        -Drelease=${fdb5_VERSION_STR}
    VERBATIM
    ${SPHINX_SOURCE} ${SPHINX_BUILD}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS
        ${SPHINX_INPUT_FILES}
        ${DOXYGEN_INDEX_FILE}
        MAIN_DEPENDENCY ${SPHINX_SOURCE}/conf.py
        COMMENT "Generating documentation with Sphinx"
)

add_custom_target(fdb-doc ALL DEPENDS ${SPHINX_INDEX_FILE})
