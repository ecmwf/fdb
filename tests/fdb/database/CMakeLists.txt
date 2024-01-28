
ecbuild_add_test( TARGET test_fdb5_database_cartesianproduct
        SOURCES test_cartesianproduct.cc
        INCLUDES ${PMEM_INCLUDE_DIRS}
        LIBS fdb5
        ENVIRONMENT "${_test_environment}")
