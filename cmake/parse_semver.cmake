###
# Function: parse_semver
#
# Description:
#   Reads a file containing a semantic version string in the format
#   MAJOR.MINOR.PATCH (e.g., 1.2.3), validates the format, and sets four
#   variables in the calling scope with the given prefix:
#
#     - <prefix>_VERSION_MAJOR
#     - <prefix>_VERSION_MINOR
#     - <prefix>_VERSION_PATCH
#     - <prefix>_VERSION_STRING
#
# Inputs:
#   - filename (string): Path to the file containing the version string.
#   - prefix   (string): Prefix to use for the output variable names.
#
# Outputs (set in PARENT_SCOPE):
#   - <prefix>_VERSION_MAJOR
#   - <prefix>_VERSION_MINOR
#   - <prefix>_VERSION_PATCH
#   - <prefix>_VERSION_STRING
#
# Errors:
#   - FATAL_ERROR is raised if the file does not exist or the contents do
#     not match the expected format.
###
function(parse_semver filename prefix)
    if(NOT EXISTS "${filename}")
        message(FATAL_ERROR "Version file '${filename}' does not exist.")
    endif()

    file(READ "${filename}" VERSION_CONTENT)
    string(STRIP "${VERSION_CONTENT}" VERSION_CONTENT)

    string(REGEX MATCH "^([0-9]+)\\.([0-9]+)\\.([0-9]+)$" _VERSION_MATCH
           "${VERSION_CONTENT}")
    if(NOT _VERSION_MATCH)
        message(FATAL_ERROR "Invalid semantic version format in '${filename}': "
                            "'${VERSION_CONTENT}'")
    endif()

    set("${prefix}_VERSION_MAJOR" "${CMAKE_MATCH_1}" PARENT_SCOPE)
    set("${prefix}_VERSION_MINOR" "${CMAKE_MATCH_2}" PARENT_SCOPE)
    set("${prefix}_VERSION_PATCH" "${CMAKE_MATCH_3}" PARENT_SCOPE)
    set("${prefix}_VERSION_STRING"
        "${CMAKE_MATCH_1}.${CMAKE_MATCH_2}.${CMAKE_MATCH_3}" PARENT_SCOPE)
endfunction()

