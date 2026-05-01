if( HAVE_FAMFDB )

    list( APPEND _fam_tests
        store catalogue engine index pipeline
    )

    if( eckit_HAVE_OPENFAM_MOCK )
        # Mock: per-process shm isolation via PID-mangled endpoints (no ECKIT_FAM_TEST_ENDPOINT).
        list( APPEND _fam_environment ${_test_environment}
            "ECKIT_FAM_MOCK_SHM_SIZE=67108864"
        )
    else()
        list( APPEND _fam_environment ${_test_environment}
            "ECKIT_FAM_TEST_ENDPOINT=172.26.0.2:8880"
        )
    endif()

    foreach( _test ${_fam_tests} )
        ecbuild_add_test(
            TARGET  fdb_test_fam_${_test}
            SOURCES test_fam_common.h test_fam_${_test}.cc
            LABELS  fam
            LIBS    fdb5
            ENVIRONMENT "${_fam_environment}"
        )
    endforeach()

endif()
