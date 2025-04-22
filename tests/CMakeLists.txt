# unit tests

include_directories( ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} )

add_subdirectory( fdb )
add_subdirectory( chunked_data_view )
add_subdirectory( regressions )

