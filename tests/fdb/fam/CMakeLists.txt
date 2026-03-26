if( HAVE_FAMFDB )

    list( APPEND _fam_tests
        store catalogue
    )

    list( APPEND _fam_environment ${_test_environment}
        "ECKIT_FAM_TEST_ENDPOINT=172.26.0.2:8880"
    )

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
