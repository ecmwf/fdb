ecbuild_add_test( TARGET fdb_debug_fakemodel
                    SOURCES debug_fakemodel.cc
                    INCLUDES ${PMEM_INCLUDE_DIRS}
                    LIBS ${PMEM_LIBRARIES} fdb5
                    ENVIRONMENT "${_test_environment}")
