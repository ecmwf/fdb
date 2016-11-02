### TODO
#
#  * Axis should consider Type
#
#  * client "database" for direct fdb5 read
#
#  * fdb5_list -class od -expver 0001
#  * fdb5_list .
#  * fdb5_purge -class rd -expver ghtf
#
#  * consider using more than a service node on the HPC
#
#  * marsadm tools:
#    --  list roots
#
#  Understand 'invalid ioctl' error
#  What happens if the mars client is killed in a mars2mars transfer
#
#
# TESTS:
# check effect on retrieve:
#  * delete a json
#  * delete a data
#  * delete a index

# MAYBE:
#  * fdbRoot is a list of paths to visit ---> speak to Hodge
#  * Rules for data files (eg {type,levtype} )
#
# Confluence documentation on how to install
# -- dont forget the ~/mars/wind
#
# Implement access control: only the user that create the toc can:
# - Add new data
# - purge
# Implement privilege users that can do everything

### config header

if( NOT FDB_HOME  )
    set( FDB_HOME "${CMAKE_INSTALL_PREFIX}" )
endif()

# fdb5 library

list( APPEND fdb5_srcs
    config/MasterConfig.cc
    config/MasterConfig.h
    config/UMask.cc
    config/UMask.h
    database/Archiver.cc
    database/Archiver.h
    database/ArchiveVisitor.cc
    database/ArchiveVisitor.h
    database/BaseArchiveVisitor.cc
    database/BaseArchiveVisitor.h
    database/DB.cc
    database/DB.h
    database/Field.cc
    database/Field.h
    database/FieldDetails.cc
    database/FieldDetails.h
    database/FieldLocation.cc
    database/FieldLocation.h
    database/FieldRef.cc
    database/FieldRef.h
    database/FileStore.cc
    database/FileStore.h
    database/Indexer.cc
    database/Indexer.h
    database/Index.cc
    database/Index.h
    database/IndexLocation.cc
    database/IndexLocation.h
    database/IndexAxis.cc
    database/IndexAxis.h
    database/IndexFactory.cc
    database/IndexFactory.h
    database/Key.cc
    database/Key.h
    database/ReadVisitor.cc
    database/ReadVisitor.h
    database/Retriever.cc
    database/Retriever.h
    database/RetrieveVisitor.cc
    database/RetrieveVisitor.h
    database/MultiRetrieveVisitor.cc
    database/MultiRetrieveVisitor.h
    database/WriteVisitor.cc
    database/WriteVisitor.h
    database/NotifyWind.cc
    database/NotifyWind.h
    grib/GribArchiver.cc
    grib/GribArchiver.h
    grib/GribDecoder.cc
    grib/GribDecoder.h
    grib/GribIndexer.cc
    grib/GribIndexer.h
    io/FDBFileHandle.cc
    io/FDBFileHandle.h
    io/HandleGatherer.cc
    io/HandleGatherer.h
    legacy/FDBIndexScanner.cc
    legacy/FDBIndexScanner.h
    legacy/LegacyArchiver.cc
    legacy/LegacyArchiver.h
    legacy/LegacyTranslator.cc
    legacy/LegacyTranslator.h
    rules/MatchAlways.cc
    rules/MatchAlways.h
    rules/MatchAny.cc
    rules/MatchAny.h
    rules/Matcher.cc
    rules/Matcher.h
    rules/MatchHidden.cc
    rules/MatchHidden.h
    rules/MatchOptional.cc
    rules/MatchOptional.h
    rules/MatchValue.cc
    rules/MatchValue.h
    rules/Predicate.cc
    rules/Predicate.h
    rules/Rule.cc
    rules/Rule.h
    rules/Schema.cc
    rules/Schema.h
    rules/SchemaParser.cc
    rules/SchemaParser.h
    toc/AdoptVisitor.cc
    toc/AdoptVisitor.h
    toc/WipeVisitor.cc
    toc/WipeVisitor.h
    toc/PurgeVisitor.cc
    toc/PurgeVisitor.h
    toc/BTreeIndex.cc
    toc/BTreeIndex.h
    toc/DbStatistics.cc
    toc/DbStatistics.h
    toc/IndexStatistics.cc
    toc/IndexStatistics.h
    toc/ReportVisitor.cc
    toc/ReportVisitor.h
    toc/TocDB.cc
    toc/TocDB.h
    toc/Root.cc
    toc/Root.h
    toc/FileSpaceHandler.cc
    toc/FileSpaceHandler.h
    toc/FileSpace.cc
    toc/FileSpace.h
    toc/ExpverFileSpaceHandler.cc
    toc/ExpverFileSpaceHandler.h
    toc/RootManager.cc
    toc/RootManager.h
    toc/TocDBReader.cc
    toc/TocDBReader.h
    toc/TocDBWriter.cc
    toc/TocDBWriter.h
    toc/TocHandler.cc
    toc/TocHandler.h
    toc/TocIndex.cc
    toc/TocIndex.h
    toc/TocIndexLocation.cc
    toc/TocIndexLocation.h
    toc/TocRecord.cc
    toc/TocRecord.h
    tools/FDBAccess.cc
    tools/FDBAccess.h
    tools/FDBInspect.cc
    tools/FDBInspect.h
    tools/FDBTool.cc
    tools/FDBTool.h
    tools/ToolRequest.cc
    tools/ToolRequest.h
    tools/RequestParser.cc
    tools/RequestParser.h
    types/Type.cc
    types/Type.h
    types/TypeAbbreviation.cc
    types/TypeAbbreviation.h
    types/TypeClimateDaily.cc
    types/TypeClimateDaily.h
    types/TypeClimateMonthly.cc
    types/TypeClimateMonthly.h
    types/TypeDate.cc
    types/TypeDate.h
    types/TypeDefault.cc
    types/TypeDefault.h
    types/TypeDouble.cc
    types/TypeDouble.cc
    types/TypeDouble.h
    types/TypeDouble.h
    types/TypeExpver.cc
    types/TypeExpver.h
    types/TypeGrid.cc
    types/TypeGrid.h
    types/TypeIgnore.cc
    types/TypeIgnore.h
    types/TypeInteger.cc
    types/TypeInteger.h
    types/TypeMonth.cc
    types/TypeMonth.h
    types/TypeParam.cc
    types/TypeParam.h
    types/TypesFactory.cc
    types/TypesFactory.h
    types/TypesRegistry.cc
    types/TypesRegistry.h
    types/TypeStep.cc
    types/TypeStep.h
    types/TypeTime.cc
    types/TypeTime.h
)

if( HAVE_PMEMFDB )
    list( APPEND fdb5_srcs
        pmem/DataPool.cc
        pmem/DataPool.h
        pmem/DataPoolManager.cc
        pmem/DataPoolManager.h
        pmem/PBaseNode.cc
        pmem/PBaseNode.h
        pmem/PBranchingNode.cc
        pmem/PBranchingNode.h
        pmem/PDataNode.cc
        pmem/PDataNode.h
        pmem/PMemDB.cc
        pmem/PMemDB.h
        pmem/PMemDBWriter.cc
        pmem/PMemDBWriter.h
        pmem/PMemIndex.cc
        pmem/PMemIndex.h
        pmem/PRoot.cc
        pmem/PRoot.h
        pmem/Pool.cc
        pmem/Pool.h
    )
else()
    set( PMEM_LIBRARIES "" )
    set( PMEM_INCLUDE_DIRS "" )
endif()

if(LUSTREAPI_FOUND)
    list( APPEND fdb5_srcs io/LustreFileHandle.h io/LustreFileHandle.c )
endif()

ecbuild_add_library(

    TARGET  mars-fdb5

    SOURCES
        ${fdb5_srcs}

    INSTALL_HEADERS LISTED

    HEADER_DESTINATION
        ${INSTALL_INCLUDE_DIR}/fdb5

    PRIVATE_INCLUDES
        ${GRIB_API_INCLUDE_DIRS}
        "${PMEM_INCLUDE_DIRS}"
        "${LUSTREAPI_INCLUDE_DIRS}"

    LIBS
        Mars
        Common
        eckit
        eckit_option
        ${grib_handling_pkg}
        ${PMEM_LIBRARIES}
        ${LUSTREAPI_LIBRARIES}
)

list( APPEND fdb5_scripts fdb grib2fdb fdb-which )

foreach( _script ${fdb5_scripts} )

configure_file(scripts/${_script}.in ${CMAKE_BINARY_DIR}/bin/${_script} @ONLY)

install(PROGRAMS ${CMAKE_BINARY_DIR}/bin/${_script}
        DESTINATION ${INSTALL_BIN_DIR} )
endforeach()


list( APPEND fdb5_tools
    fdb-adopt
    fdb-write
    fdb-dump
    fdb-list
    fdb-patch
    fdb-purge
    fdb-stats
    fdb-read
    fdb-schema
    fdb-where
    fdb-wipe
    fdb-root
)

foreach( _tool ${fdb5_tools} )

  ecbuild_add_executable( TARGET    ${_tool}
                          SOURCES   tools/${_tool}.cc
                          INCLUDES  ${GRIB_API_INCLUDE_DIRS} # Please don't remove me, I am needed
                          LIBS
                          mars-fdb5 )

endforeach()


ecbuild_add_executable( TARGET    grib2fdb5
                          SOURCES   compat/grib2fdb5.cc
                          INCLUDES  ${GRIB_API_INCLUDE_DIRS} # Please don't remove me, I am needed
                          LIBS
                          mars-fdb5 )


