# unit tests

find_package(Pytest REQUIRED)

include_directories( ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} )

add_subdirectory( fdb )
add_subdirectory( pyfdb )
add_subdirectory( regressions )

