if( eckit_HAVE_OPENFAM_MOCK )
    list( APPEND _fam_environment
        "ECKIT_FAM_MOCK_SHM_SIZE=2684354560"
    )
else()
    list( APPEND _fam_environment ${_test_environment}
        "OPENFAM_ROOT=/workspace/cis-rpc_meta-direct_mem-rpc"
        "FDB_FAM_TEST_ENDPOINT=172.26.0.2:8880"
    )
endif()

configure_file( fdb_fam.sh.in fdb_fam.sh @ONLY )

ecbuild_add_test(
    TARGET fdb_test_tools_fam
    TYPE SCRIPT
    LABELS fam tools
    CONDITION HAVE_FDB_BUILD_TOOLS
    ENVIRONMENT "${_fam_environment}"
    COMMAND fdb_fam.sh
)

list( APPEND fdb_tools_fam_tests
    inspect
    list_all
    list_all_full
    list_masking
    list_minimum_keys
    list_porcelain
    list_ranges
    dump
)

foreach( _test ${fdb_tools_fam_tests} )
    configure_file( fdb_${_test}_fam.sh.in fdb_${_test}_fam.sh @ONLY )

    ecbuild_add_test(
        TARGET fdb_test_tools_list_${_test}_fam
        TYPE SCRIPT
        LABELS fam tools
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_GRIB
        TEST_DEPENDS fdb_test_tools_fam fdb_test_get_data
        COMMAND fdb_${_test}_fam.sh
        ENVIRONMENT "${_fam_environment}" )
endforeach()
