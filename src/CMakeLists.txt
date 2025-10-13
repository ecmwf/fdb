############################################################################################
# libraries

ecbuild_find_project_files()

add_subdirectory(fdb5)
add_subdirectory(chunked_data_view)

if (HAVE_PYTHON_ZARR_INTERFACE)
    add_subdirectory(chunked_data_view_bindings)
endif()

if (HAVE_DUMMY_DAOS)
	add_subdirectory(dummy_daos)
endif()

############################################################################################
# Following directories are not packaged
# currently this is developer only, hence not distributed

if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/sandbox/CMakeLists.txt)
	add_subdirectory(sandbox)
endif()

