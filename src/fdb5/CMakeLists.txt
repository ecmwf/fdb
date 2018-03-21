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
	LibFdb.h
	LibFdb.cc
    config/UMask.cc
    config/UMask.h
    config/FDBConfig.cc
    config/FDBConfig.h
    database/Archiver.cc
    database/Archiver.h
    database/ArchiveVisitor.cc
    database/ArchiveVisitor.h
    database/BaseArchiveVisitor.cc
    database/BaseArchiveVisitor.h
    database/DB.cc
    database/DB.h
    database/DataStats.cc
    database/DataStats.h
    database/DbStats.cc
    database/DbStats.h
    database/Engine.cc
    database/Engine.h
    database/Field.cc
    database/Field.h
    database/FieldDetails.cc
    database/FieldDetails.h
    database/FieldLocation.cc
    database/FieldLocation.h
    database/FileStore.cc
    database/FileStore.h
    database/Indexer.cc
    database/Indexer.h
    database/Index.cc
    database/Index.h
    database/IndexLocation.cc
    database/IndexLocation.h
    database/IndexStats.cc
    database/IndexStats.h
    database/StatsVisitor.cc
    database/StatsVisitor.h
    database/IndexAxis.cc
    database/IndexAxis.h
    database/IndexFactory.cc
    database/IndexFactory.h
    database/Key.cc
    database/Key.h
    database/ReadVisitor.cc
    database/ReadVisitor.h
    database/ReportVisitor.cc
    database/ReportVisitor.h
    database/Report.cc
    database/Report.h
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
    database/Manager.cc
    database/Manager.h
    grib/GribArchiver.cc
    grib/GribArchiver.h
    grib/GribDecoder.cc
    grib/GribDecoder.h
    grib/GribIndexer.cc
    grib/GribIndexer.h
    io/FDBFileHandle.cc
    io/FDBFileHandle.h
    io/fdb5_lustreapi_file_create.c
    io/LustreFileHandle.cc
    io/LustreFileHandle.h
    io/HandleGatherer.cc
    io/HandleGatherer.h
    legacy/FDBIndexScanner.cc
    legacy/FDBIndexScanner.h
    legacy/LegacyRetriever.cc
    legacy/LegacyRetriever.h
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

if( HAVE_TOCFDB )
    list( APPEND fdb5_srcs
        toc/AdoptVisitor.cc
        toc/AdoptVisitor.h
        toc/WipeVisitor.cc
        toc/WipeVisitor.h
        toc/PurgeVisitor.cc
        toc/PurgeVisitor.h
        toc/BTreeIndex.cc
        toc/BTreeIndex.h
        toc/TocDB.cc
        toc/TocDB.h
        toc/Root.cc
        toc/Root.h
        toc/FieldRef.cc
        toc/FieldRef.h
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
        toc/TocFieldLocation.cc
        toc/TocFieldLocation.h
        toc/TocHandler.cc
        toc/TocHandler.h
        toc/TocIndex.cc
        toc/TocIndex.h
        toc/TocIndexLocation.cc
        toc/TocIndexLocation.h
        toc/TocRecord.cc
        toc/TocRecord.h
        toc/TocStats.cc
        toc/TocStats.h
        toc/TocEngine.cc
        toc/TocEngine.h
        toc/TocReportVisitor.cc
        toc/TocReportVisitor.h
        )

    list( APPEND fdb5_tools
        fdb-wipe
        fdb-stats
        fdb-purge
        fdb-dump-toc
        fdb-dump-index )

endif()

if( HAVE_PMEMFDB )

    list( APPEND fdb5_srcs
        pmem/DataPool.cc
        pmem/DataPool.h
        pmem/DataPoolManager.cc
        pmem/DataPoolManager.h
        pmem/MemoryBufferStream.h
        pmem/MemoryBufferStream.cc
        pmem/PBaseNode.cc
        pmem/PBaseNode.h
        pmem/PBranchingNode.cc
        pmem/PBranchingNode.h
        pmem/PDataNode.cc
        pmem/PDataNode.h
        pmem/PDataRoot.cc
        pmem/PDataRoot.h
        pmem/PIndexRoot.cc
        pmem/PIndexRoot.h
        pmem/PMemDB.cc
        pmem/PMemDB.h
        pmem/PMemDBReader.cc
        pmem/PMemDBReader.h
        pmem/PMemDBWriter.cc
        pmem/PMemDBWriter.h
        pmem/PMemFieldLocation.cc
        pmem/PMemFieldLocation.h
        pmem/PMemIndex.cc
        pmem/PMemIndex.h
        pmem/PMemIndexLocation.cc
        pmem/PMemIndexLocation.h
        pmem/PRoot.cc
        pmem/PRoot.h
        pmem/Pool.cc
        pmem/Pool.h
        pmem/PoolEntry.cc
        pmem/PoolEntry.h
        pmem/PoolGroup.cc
        pmem/PoolGroup.h
        pmem/PoolManager.cc
        pmem/PoolManager.h
        pmem/PMemStats.cc
        pmem/PMemStats.h
        pmem/PMemEngine.cc
        pmem/PMemEngine.h
    )


else()
    set( PMEM_LIBRARIES "" )
    set( PMEM_INCLUDE_DIRS "" )
endif()


if( HAVE_DISTFDB )
    list( APPEND fdb5_srcs
        dist/DistDB.cc
        dist/DistDB.h
        dist/DistManager.cc
        dist/DistManager.h
        dist/DistDBReader.cc
        dist/DistDBReader.h
        dist/DistDBWriter.cc
        dist/DistDBWriter.h
        dist/DistEngine.cc
        dist/DistEngine.h
    )
endif()

if(LUSTREAPI_FOUND)
    list( APPEND fdb5_srcs io/LustreFileHandle.h io/LustreFileHandle.cc )
endif()

ecbuild_add_library(

    TARGET  mars-fdb5

    SOURCES
        ${fdb5_srcs}

    INSTALL_HEADERS LISTED

    HEADER_DESTINATION
        ${INSTALL_INCLUDE_DIR}/fdb5

    PRIVATE_INCLUDES
        "${GRIB_API_INCLUDE_DIRS}"
        "${PMEM_INCLUDE_DIRS}"
        "${LUSTREAPI_INCLUDE_DIRS}"

    LIBS
        Mars
        Common
        metkit
        eckit
        eckit_option
        ${grib_handling_pkg}
        ${PMEM_LIBRARIES}
        ${LUSTREAPI_LIBRARIES}
)

list( APPEND fdb5_scripts fdb fdb-which )

foreach( _script ${fdb5_scripts} )

configure_file(scripts/${_script}.in ${CMAKE_BINARY_DIR}/bin/${_script} @ONLY)

install(PROGRAMS ${CMAKE_BINARY_DIR}/bin/${_script}
        DESTINATION ${INSTALL_BIN_DIR} )
endforeach()


list( APPEND fdb5_tools
    fdb-adopt
    fdb-write
    fdb-write-legacy
    fdb-dump
    fdb-list
    fdb-patch
    fdb-read
    fdb-schema
    fdb-where
    fdb-root
    fdb-info
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


