if( HAVE_PMEMFDB )

    ecbuild_add_test( TARGET test_fdb5_pmem_pdatanode
                      SOURCES test_pdatanode.cc
                      INCLUDES ${PMEM_INCLUDE_DIRS}
                      LIBS ${PMEM_LIBRARIES} mars-fdb5
                      ENVIRONMENT "${_test_environment}")

    ecbuild_add_test( TARGET test_fdb5_pmem_pbranchingnode
                      SOURCES test_pbranchingnode.cc
                      INCLUDES ${PMEM_INCLUDE_DIRS}
                      LIBS ${PMEM_LIBRARIES} mars-fdb5
                      ENVIRONMENT "${_test_environment}")

    ecbuild_add_test( TARGET test_fdb5_pmem_pbasenode
                      SOURCES test_pbasenode.cc
                      INCLUDES ${PMEM_INCLUDE_DIRS}
                      LIBS ${PMEM_LIBRARIES} mars-fdb5
                      ENVIRONMENT "${_test_environment}")

    ecbuild_add_test( TARGET test_fdb5_pmem_pool_manager
                      SOURCES test_pool_manager.cc
                      INCLUDES ${PMEM_INCLUDE_DIRS}
                      LIBS ${PMEM_LIBRARIES} mars-fdb5
                      ENVIRONMENT "${_test_environment}")

endif()
