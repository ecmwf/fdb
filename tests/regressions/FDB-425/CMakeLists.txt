ecbuild_add_test( TARGET FDB-425-purge
                    SOURCES test_subtoc_purge.cc
                    INCLUDES ${PMEM_INCLUDE_DIRS}
                    LIBS ${PMEM_LIBRARIES} fdb5
                    ENVIRONMENT "${test_environment}")

# copy the schema file to the test build directory
configure_file( ${CMAKE_CURRENT_SOURCE_DIR}/fdb-425-schema ${CMAKE_CURRENT_BINARY_DIR}/fdb-425-schema COPYONLY )