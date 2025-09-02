if( HAVE_FAMFDB )

    list( APPEND _fam_tests
        store catalogue
    )

    foreach( _test ${_fam_tests} )
        ecbuild_add_test(
            TARGET  test_fdb5_fam_${_test}
            SOURCES test_fam_common.h test_fam_${_test}.cc
            LIBS    fdb5
            ENVIRONMENT "${_test_environment}"
        )
    endforeach()

endif()
