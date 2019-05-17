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

ecbuild_generate_config_headers( ${INSTALL_INCLUDE_DIR}/fdb5 )

configure_file( fdb5_config.h.in    fdb5_config.h   )
configure_file( fdb5_version.h.in   fdb5_version.h  )
configure_file( fdb5_version.c.in   fdb5_version.c   )

install( FILES ${CMAKE_CURRENT_BINARY_DIR}/fdb5_config.h
               ${CMAKE_CURRENT_BINARY_DIR}/fdb5_version.h
         DESTINATION ${INSTALL_INCLUDE_DIR}/fdb5 )

# fdb5 library

list( APPEND fdb5_srcs
    fdb5_version.c
	LibFdb5.h
	LibFdb5.cc

    api/DistFDB.cc
    api/DistFDB.h
    api/FDB.cc
    api/FDB.h
    api/FDBFactory.cc
    api/FDBFactory.h
    api/FDBStats.cc
    api/FDBStats.h
    api/LocalFDB.cc
    api/LocalFDB.h
    api/SelectFDB.cc
    api/SelectFDB.h

    api/helpers/APIIterator.h
    api/helpers/FDBToolRequest.cc
    api/helpers/FDBToolRequest.h
    api/helpers/DumpIterator.h
    api/helpers/ListIterator.cc
    api/helpers/ListIterator.h
    api/helpers/WhereIterator.h
    api/helpers/WipeIterator.cc
    api/helpers/WipeIterator.h
    api/helpers/PurgeIterator.h
    api/helpers/StatsIterator.cc
    api/helpers/StatsIterator.h

    api/local/QueryVisitor.h
    api/local/QueueStringLogTarget.h
    api/local/ListVisitor.h
    api/local/DumpVisitor.h
    api/local/WhereVisitor.h
    api/local/WipeVisitor.cc
    api/local/WipeVisitor.h
    api/local/PurgeVisitor.cc
    api/local/PurgeVisitor.h
    api/local/StatsVisitor.cc
    api/local/StatsVisitor.h

    config/UMask.cc
    config/UMask.h
    config/Config.cc
    config/Config.h
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
    database/EntryVisitMechanism.cc
    database/EntryVisitMechanism.h
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
    database/StatsReportVisitor.cc
    database/StatsReportVisitor.h
    database/PurgeVisitor.cc
    database/PurgeVisitor.h
    database/IndexAxis.cc
    database/IndexAxis.h
    database/IndexFactory.cc
    database/IndexFactory.h
    database/Key.cc
    database/Key.h
    database/ReadVisitor.cc
    database/ReadVisitor.h
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
    database/Notifier.cc
    database/Notifier.h
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
    io/SingleGribMungePartFileHandle.cc
    io/SingleGribMungePartFileHandle.h
    io/LustreFileHandle.cc
    io/LustreFileHandle.h
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
    tools/FDBTool.cc
    tools/FDBTool.h
    tools/FDBVisitTool.cc
    tools/FDBVisitTool.h
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
        toc/EnvVarFileSpaceHandler.cc
        toc/EnvVarFileSpaceHandler.h
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
        toc/TocPurgeVisitor.cc
        toc/TocPurgeVisitor.h
        toc/TocRecord.cc
        toc/TocRecord.h
        toc/TocStats.cc
        toc/TocStats.h
        toc/TocEngine.cc
        toc/TocEngine.h
        )

    list( APPEND fdb5_tools
        fdb-wipe
        fdb-stats
        fdb-purge
        fdb-dump-toc
        fdb-dump-index
        fdb-reconsolidate-toc )

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

    list( APPEND fdb5_srcs
        api/RemoteFDB.cc
        api/RemoteFDB.h
        remote/RemoteFieldLocation.h
        remote/RemoteFieldLocation.cc
    )

if(HAVE_LUSTRE)
    list( APPEND fdb5_srcs io/LustreFileHandle.h io/LustreFileHandle.cc )
endif()

ecbuild_add_library(

    TARGET  fdb5

    SOURCES
        ${fdb5_srcs}

    GENERATED
        fdb5_version.c

    INSTALL_HEADERS LISTED

    HEADER_DESTINATION
        ${INSTALL_INCLUDE_DIR}/fdb5

    PRIVATE_INCLUDES
        "${ECCODES_INCLUDE_DIRS}"
        "${PMEM_INCLUDE_DIRS}"
        "${LUSTREAPI_INCLUDE_DIRS}"

    LIBS
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
    fdb-hammer
    fdb-write
    fdb-dump
    fdb-list
    fdb-patch
    fdb-read
    fdb-schema
    fdb-where
    fdb-info
)

if ( HAVE_TOCFDB )
    list(APPEND fdb5_tools
        fdb-adopt
        fdb-write-legacy
        fdb-root
        fdb-overlay
        fdb-hide
    )
endif()

foreach( _tool ${fdb5_tools} )

  ecbuild_add_executable( TARGET    ${_tool}
                          SOURCES   tools/${_tool}.cc
                          INCLUDES  ${ECCODES_INCLUDE_DIRS}  # Please don't remove me, I am needed
                          LIBS      fdb5 )

endforeach()


#ecbuild_add_executable( TARGET    fdb-partial-adopt
                            #SOURCES   tools/fdb-partial-adopt.cc
                            #CONDITION HAVE_FDB4_MIGRATION
                            ##INCLUDES  ${FDB_INCLUDE_DIRS}
                            #LIBS      fdb5 fdb )

ecbuild_add_executable( TARGET    grib2fdb5
                        SOURCES   compat/grib2fdb5.cc
                        INCLUDES  ${ECCODES_INCLUDE_DIRS} # Please don't remove me, I am needed
                        LIBS      fdb5 )

ecbuild_add_executable(
        TARGET fdb-monitor
        SOURCES remote/fdb-monitor.cc
        LIBS eckit eckit_cmd )

ecbuild_add_executable(
        TARGET fdbsvr
        SOURCES
            remote/fdb-server.cc
            remote/Messages.h
            remote/Messages.cc
            remote/Handler.h
            remote/Handler.cc
            remote/AvailablePortList.cc
            remote/AvailablePortList.h
        LIBS fdb5 )
