if( HAVE_FAMFDB )

    # only relevant for testing with a real OpenFAM servers running in docker environment.
    list( APPEND _fam_environment ${_test_environment}
        "OPENFAM_ROOT=/workspace/cis-rpc_meta-direct_mem-rpc"
        "FDB_FAM_TEST_ENDPOINT=172.26.0.2:8880"
    )

    configure_file( fdb_fam.sh.in fdb_fam.sh @ONLY )

    ecbuild_add_test(
        TARGET fdb_test_tools_fam
        TYPE SCRIPT
        LABELS fam
        ENVIRONMENT "${_fam_environment}"
        COMMAND fdb_fam.sh
    )

    if( HAVE_GRIB )
        configure_file( fdb_dump_fam.sh.in fdb_dump_fam.sh @ONLY )

        ecbuild_add_test(
            TARGET fdb_test_tools_dump_fam
            TYPE SCRIPT
            LABELS fam
            COMMAND fdb_dump_fam.sh
            ENVIRONMENT "${_fam_environment}" )
    endif()

endif()
