# Only the mock shares a size-limited /dev/shm segment; real OpenFAM has no such constraint.
set( _fam_test_properties "" )

if( eckit_HAVE_OPENFAM_MOCK )
    # The mock State metadata is ~22 MiB; keep the segment modest so one test fits comfortably in
    # the (commonly 64 MiB) /dev/shm. Concurrency is bounded by RESOURCE_LOCK on each test below.
    list( APPEND _fam_environment ${_test_environment}
        "ECKIT_FAM_MOCK_SHM_SIZE=33554432"
    )
    # Serialise FAM tests: each mock segment reserves ~32 MiB of the shared, size-limited /dev/shm,
    # so running many at once (ctest -j) exhausts tmpfs.
    set( _fam_test_properties TEST_PROPERTIES RESOURCE_LOCK famfdb_mock_shm )
elseif( eckit_HAVE_OPENFAM )
    # Real OpenFAM servers are site-specific, so the caller must point the tests at them.
    if( NOT DEFINED ENV{OPENFAM_ROOT} OR NOT DEFINED ENV{ECKIT_FAM_TEST_ENDPOINT} )
        ecbuild_warn( "FAM tests need OPENFAM_ROOT and ECKIT_FAM_TEST_ENDPOINT in the environment; skipping them." )
        return()
    endif()
    list( APPEND _fam_environment ${_test_environment}
        "OPENFAM_ROOT=$ENV{OPENFAM_ROOT}"
        "ECKIT_FAM_TEST_ENDPOINT=$ENV{ECKIT_FAM_TEST_ENDPOINT}"
    )
endif()

configure_file( fdb_fam.sh.in fdb_fam.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_fam
    TYPE SCRIPT
    LABELS fam tools
    CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_FAMFDB
    ENVIRONMENT "${_fam_environment}"
    ${_fam_test_properties}
    COMMAND fdb_fam.sh
)

list( APPEND fdb_tools_fam_tests
    inspect
    list_all
    list_all_full
    list_minimum_keys
    list_porcelain
    dump
)

foreach( _test ${fdb_tools_fam_tests} )
    configure_file( fdb_${_test}_fam.sh.in fdb_${_test}_fam.sh @ONLY )

    ecbuild_add_test(
        TARGET fdb_test_tools_${_test}_fam
        TYPE SCRIPT
        LABELS fam tools
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB AND HAVE_FAMFDB
        TEST_DEPENDS fdb_test_tools_fam fdb_test_get_data
        COMMAND fdb_${_test}_fam.sh
        ENVIRONMENT "${_fam_environment}"
        ${_fam_test_properties} )
endforeach()

list( APPEND fdb_tools_fam_maintenance_tests
    stats
    purge
)

foreach( _test ${fdb_tools_fam_maintenance_tests} )
    configure_file( fdb_${_test}_fam.sh.in fdb_${_test}_fam.sh @ONLY )

    ecbuild_add_test(
        TARGET fdb_test_tools_${_test}_fam
        TYPE SCRIPT
        LABELS fam tools
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB AND HAVE_FAMFDB
        TEST_DEPENDS fdb_test_tools_fam fdb_test_get_data
        COMMAND fdb_${_test}_fam.sh
        ENVIRONMENT "${_fam_environment}"
        ${_fam_test_properties} )
endforeach()
