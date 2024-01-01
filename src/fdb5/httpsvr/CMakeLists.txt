
set(plugin_name "fdb5-httpsvr")

ecbuild_add_library(
    TARGET       ${plugin_name}-plugin
    SOURCES      LibHttpSvrFdb5.cc
                 LibHttpSvrFdb5.h
                 Navigator.cc
                 Navigator.h
                 TreeBrowser.cc
                 TreeBrowser.h
    PUBLIC_LIBS  eckit_web metkit fdb5
)

# plugin self-registation file
configure_file(${plugin_name}.json ${CMAKE_BINARY_DIR}/share/plugins/int.ecmwf.${plugin_name}.json @ONLY)
install(FILES         ${CMAKE_BINARY_DIR}/share/plugins/int.ecmwf.${plugin_name}.json
        DESTINATION   "share/plugins" )
