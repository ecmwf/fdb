if( HAVE_FAMFDB )

    list( APPEND _fam_tests
        store catalogue engine index pipeline stats
    )

    # Only the mock shares a size-limited /dev/shm segment; real OpenFAM has no such constraint.
    set( _fam_test_properties "" )

    if( eckit_HAVE_OPENFAM_MOCK )
        # Mock: per-process shm isolation via PID-mangled endpoints (no ECKIT_FAM_TEST_ENDPOINT).
        # The mock State metadata is ~22 MiB; keep the segment modest so one test fits comfortably
        # in the (commonly 64 MiB) /dev/shm. Concurrency is bounded by RESOURCE_LOCK below.
        list( APPEND _fam_environment ${_test_environment}
            "ECKIT_FAM_MOCK_SHM_SIZE=33554432"
        )
        # Serialise FAM tests: each mock segment reserves ~32 MiB of the shared, size-limited
        # /dev/shm, so running many at once (ctest -j) exhausts tmpfs.
        set( _fam_test_properties TEST_PROPERTIES RESOURCE_LOCK famfdb_mock_shm )
    else()
        if( DEFINED ENV{ECKIT_FAM_TEST_ENDPOINT} )
            set( _famfdb_fam_endpoint "$ENV{ECKIT_FAM_TEST_ENDPOINT}" )
        else()
            set( _famfdb_fam_endpoint "172.24.0.2:8880" )
        endif()
        list( APPEND _fam_environment ${_test_environment}
            "ECKIT_FAM_TEST_ENDPOINT=${_famfdb_fam_endpoint}"
        )
    endif()

    foreach( _test ${_fam_tests} )
        ecbuild_add_test(
            TARGET  fdb_test_fam_${_test}
            SOURCES test_fam_common.h test_fam_${_test}.cc
            LABELS  fam
            LIBS    fdb5
            ENVIRONMENT "${_fam_environment}"
            ${_fam_test_properties}
        )
    endforeach()

endif()
