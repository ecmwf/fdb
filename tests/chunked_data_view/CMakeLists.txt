list(APPEND tests
    view
    axis
    index_mapper
    builder
)

foreach(test ${tests})
    ecbuild_add_test(
        TARGET fdb_test_chunked_data_view_${test}
        SOURCES test_${test}.cc
        LIBS 
            chunked_data_view
            fdb5
            eckit
            metkit
    )
endforeach()

