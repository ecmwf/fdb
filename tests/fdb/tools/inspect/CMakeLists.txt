set(test_name fdb_inspect)

configure_file( ${test_name}.sh.in ${test_name}.sh @ONLY )

ecbuild_add_test(
    TYPE SCRIPT
    CONDITION HAVE_FDB_BUILD_TOOLS
    COMMAND ${test_name}.sh
    ENVIRONMENT "${test_environment}")
