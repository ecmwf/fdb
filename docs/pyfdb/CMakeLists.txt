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


# Testing of docs

function(add_rst_doc_test test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME pyfdb_doc_${stem}
        COMMAND ${Python_INTERPRETER}
            -m pytest
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test}
    )
    set(_env
        "PYTHONPATH=${CMAKE_BINARY_DIR}/pyfdb-python-package-staging:${PYTHONPATH}"
        #prevent findlibs in pyfdb to pickup any globally installed fdb
        "FDB5_DIR=${CMAKE_BINARY_DIR}"
        #prevent findlibs in eccodes-python to pickup any globally installed eccodes
        "ECCODES_PYTHON_USE_FINDLIBS=1"
        "ECCODES_DIR=${CMAKE_BINARY_DIR}"
    )
    set_tests_properties(pyfdb_doc_${stem}
        PROPERTIES
        RESOURCE_LOCK pytest
        ENVIRONMENT "${_env}"
        LABELS pyfdb_doc
    )
endfunction()


set(doc_test_files
    examples.rst
)

foreach(test_file ${doc_test_files})
    add_rst_doc_test(${test_file})
endforeach()

