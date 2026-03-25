if( HAVE_FAMFDB )

    configure_file( fdb_fam.sh.in fdb_fam.sh @ONLY )

    ecbuild_add_test(
        TARGET fdb_test_tools_fam
        TYPE SCRIPT
        LABELS fam
        COMMAND fdb_fam.sh
        ENVIRONMENT "${test_environment}" "ECKIT_FAM_TEST_ENDPOINT=172.26.0.2:8880" )

endif()
