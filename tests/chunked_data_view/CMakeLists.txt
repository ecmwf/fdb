list(APPEND tests
    view
    axis
    index_mapper
    builder
)

foreach(test ${tests})
    ecbuild_add_test(
        TARGET chunked_data_view_test_${test}
        SOURCES test_${test}.cc
        LIBS 
            chunked_data_view
            fdb5
            eckit
            metkit
    )
endforeach()

