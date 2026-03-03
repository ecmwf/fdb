# Generate tests using different configs:
# 1. "Classic" case where the catalogue and store share the same root
# 2. Case where the catalogue and store have separate roots

list( APPEND fdb_tools_wipe_configs
    shared_root
    separate_cat_store
)

configure_file( wipe.sh.in wipe.sh @ONLY )

foreach( _test ${fdb_tools_wipe_configs} )
    ecbuild_add_test(
        TARGET fdb_test_tools_wipe_${_test}
        TYPE SCRIPT
        LABELS wipe
        TEST_DEPENDS fdb_test_get_data
        COMMAND wipe.sh
        ARGS ${_test}.yaml
        ENVIRONMENT "${test_environment}"
        TEST_PROPERTIES
        RESOURCE_LOCK fdb_tools_wipe_lock
        )

endforeach( )
