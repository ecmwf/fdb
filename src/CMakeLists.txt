############################################################################################
# libraries

ecbuild_find_project_files()

add_subdirectory( fdb5 )

if (HAVE_DUMMY_DAOS)
	add_subdirectory(dummy_daos)
endif()

############################################################################################
# following directories are not packaged
# currently this is developer only, hence not distributed

if( EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/sandbox/CMakeLists.txt )
	add_subdirectory( sandbox  )
endif()

