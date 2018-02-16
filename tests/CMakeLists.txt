# unit tests

include_directories( ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} )

# persistent layer classes

add_subdirectory( persistent )

# fdb

add_subdirectory( fdb )

# tests for logging with mars-server

add_subdirectory( log )

