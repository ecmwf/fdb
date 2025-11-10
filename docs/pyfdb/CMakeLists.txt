find_package(Sphinx REQUIRED)

file(GLOB_RECURSE SPHINX_INPUT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.rst)
set(SPHINX_INDEX_FILE ${CMAKE_CURRENT_BINARY_DIR}/sphinx/index.html)

add_custom_command(
    DEPENDS 
        ${SPHINX_INPUT_FILES}
        conf.py
    COMMAND
        SPHINX_EXECUTABLE=${SPHINX_EXECUTABLE}
        DOCBUILD_OUTPUT=${CMAKE_CURRENT_BINARY_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/build_docs.sh
    COMMENT "Generating documentation for pyfdb"
    OUTPUT ${SPHINX_INDEX_FILE}
)

add_custom_target(pyfdb-doc ALL DEPENDS ${SPHINX_INDEX_FILE})

