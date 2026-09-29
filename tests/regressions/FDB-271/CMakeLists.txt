ecbuild_configure_file( FDB-271.sh.in FDB-271.sh @ONLY )

CHECK_TYPE_SIZE(off_t SIZEOF_OFF_T)

if ("${SIZEOF_OFF_T}" LESS 8)
    ecbuild_add_test(
        TYPE     SCRIPT
        COMMAND  FDB-271.sh
        ENVIRONMENT "${test_environment}" )
endif()
