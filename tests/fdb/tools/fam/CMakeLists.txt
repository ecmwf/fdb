if( eckit_HAVE_OPENFAM_MOCK )
    list( APPEND _fam_environment ${_test_environment}
        "ECKIT_FAM_MOCK_SHM_SIZE=67108864"
    )
elseif( eckit_HAVE_OPENFAM )
    if( DEFINED ENV{OPENFAM_ROOT} )
        set( _famfdb_openfam_root "$ENV{OPENFAM_ROOT}" )
    else()
        set( _famfdb_openfam_root "/workspace/cis-rpc_meta-direct_mem-rpc" )
    endif()
    if( DEFINED ENV{ECKIT_FAM_TEST_ENDPOINT} )
        set( _famfdb_fam_endpoint "$ENV{ECKIT_FAM_TEST_ENDPOINT}" )
    else()
        set( _famfdb_fam_endpoint "172.24.0.2:8880" )
    endif()
    list( APPEND _fam_environment ${_test_environment}
        "OPENFAM_ROOT=${_famfdb_openfam_root}"
        "ECKIT_FAM_TEST_ENDPOINT=${_famfdb_fam_endpoint}"
    )
endif()

configure_file( fdb_fam.sh.in fdb_fam.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_fam
    TYPE SCRIPT
    LABELS fam tools
    CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_FAMFDB
    ENVIRONMENT "${_fam_environment}"
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
        TARGET fdb_test_tools_list_${_test}_fam
        TYPE SCRIPT
        LABELS fam tools
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB AND HAVE_FAMFDB
        TEST_DEPENDS fdb_test_tools_fam fdb_test_get_data
        COMMAND fdb_${_test}_fam.sh
        ENVIRONMENT "${_fam_environment}" )
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
        ENVIRONMENT "${_fam_environment}" )
endforeach()
