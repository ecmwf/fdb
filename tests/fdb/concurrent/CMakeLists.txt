ecbuild_configure_file( tsan_suppressions.txt.in tsan_suppressions.txt @ONLY )

list( APPEND _concurrent_environment
    ${test_environment}
    "FDB_HOME=${PROJECT_BINARY_DIR}"
    "TEST_FDB_THREAD_COUNT=8"
    "TEST_FDB_PROCESS_COUNT=8"
    "TSAN_OPTIONS=suppressions=${CMAKE_CURRENT_BINARY_DIR}/tsan_suppressions.txt"
)

foreach( _test process thread )
  ecbuild_add_test( TARGET      fdb_test_${_test}
                    SOURCES     test_${_test}.cc
                    LABELS      fdb concurrent
                    ENVIRONMENT "${_concurrent_environment}"
                    LIBS        fdb5 )
endforeach()
