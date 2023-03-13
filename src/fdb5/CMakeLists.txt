ecbuild_generate_config_headers( DESTINATION ${INSTALL_INCLUDE_DIR}/fdb5 )

configure_file( fdb5_config.h.in    fdb5_config.h   )
configure_file( fdb5_version.h.in   fdb5_version.h  )
configure_file( fdb5_version.c.in   ${CMAKE_CURRENT_BINARY_DIR}/fdb5_version.c   )

install( FILES ${CMAKE_CURRENT_BINARY_DIR}/fdb5_config.h
               ${CMAKE_CURRENT_BINARY_DIR}/fdb5_version.h
         DESTINATION ${INSTALL_INCLUDE_DIR}/fdb5 )

# fdb5 library

list( APPEND fdb5_srcs
    ${CMAKE_CURRENT_BINARY_DIR}/fdb5_version.c
	LibFdb5.h
	LibFdb5.cc

    api/fdb_c.cc
    api/fdb_c.h
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
    api/RandomFDB.cc
    api/SelectFDB.cc
    api/SelectFDB.h

    api/helpers/APIIterator.h
    api/helpers/ControlIterator.cc
    api/helpers/ControlIterator.h
    api/helpers/FDBToolRequest.cc
    api/helpers/FDBToolRequest.h
    api/helpers/DumpIterator.h
    api/helpers/ListIterator.cc
    api/helpers/ListIterator.h
    api/helpers/LockIterator.h
    api/helpers/MoveIterator.h
    api/helpers/StatusIterator.h
    api/helpers/WipeIterator.h
    api/helpers/PurgeIterator.h
    api/helpers/StatsIterator.cc
    api/helpers/StatsIterator.h

    api/local/QueryVisitor.h
    api/local/QueueStringLogTarget.h
    api/local/ListVisitor.h
    api/local/ControlVisitor.cc
    api/local/ControlVisitor.h
    api/local/DumpVisitor.h
    api/local/WipeVisitor.cc
    api/local/WipeVisitor.h
    api/local/MoveVisitor.cc
    api/local/MoveVisitor.h
    api/local/PurgeVisitor.cc
    api/local/PurgeVisitor.h
    api/local/StatsVisitor.cc
    api/local/StatsVisitor.h
    api/local/StatusVisitor.h

    config/Config.cc
    config/Config.h
    database/Archiver.cc
    database/Archiver.h
    database/ArchiveVisitor.cc
    database/ArchiveVisitor.h
    database/AxisRegistry.cc
    database/AxisRegistry.h
    database/BaseArchiveVisitor.cc
    database/BaseArchiveVisitor.h
    database/Catalogue.cc
    database/Catalogue.h
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
    database/UriStore.cc
    database/UriStore.h
    database/Indexer.cc
    database/Indexer.h
    database/Index.cc
    database/Index.h
    database/IndexLocation.cc
    database/IndexLocation.h
    database/IndexStats.cc
    database/IndexStats.h
    database/Inspector.cc
    database/Inspector.h
    database/StatsReportVisitor.cc
    database/StatsReportVisitor.h
    database/Store.cc
    database/Store.h
    database/PurgeVisitor.cc
    database/PurgeVisitor.h
    database/WipeVisitor.cc
    database/WipeVisitor.h
    database/MoveVisitor.cc
    database/MoveVisitor.h
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
    message/MessageArchiver.cc
    message/MessageArchiver.h
    message/MessageDecoder.cc
    message/MessageDecoder.h
    message/MessageIndexer.cc
    message/MessageIndexer.h
    io/FDBFileHandle.cc
    io/FDBFileHandle.h
    io/LustreSettings.cc
    io/LustreSettings.h
    io/LustreFileHandle.h
    io/HandleGatherer.cc
    io/HandleGatherer.h
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

if(HAVE_FDB_REMOTE)
    list( APPEND fdb5_srcs 
        api/RemoteFDB.cc
        api/RemoteFDB.h

        remote/RemoteConfiguration.h
        remote/RemoteConfiguration.cc
        remote/RemoteFieldLocation.h
        remote/RemoteFieldLocation.cc
        remote/Messages.h
        remote/Messages.cc
        remote/Handler.h
        remote/Handler.cc
        remote/AvailablePortList.cc
        remote/AvailablePortList.h
        remote/FdbServer.h
        remote/FdbServer.cc
    )
endif()

if(fdb5_HAVE_LUSTRE)
  list( APPEND fdb5_srcs io/fdb5_lustreapi_file_create.c )
endif()

if ( HAVE_GRIB )
    list( APPEND fdb5_srcs
        io/SingleGribMungePartFileHandle.cc
        io/SingleGribMungePartFileHandle.h
    )
endif()

if( HAVE_TOCFDB )
    list( APPEND fdb5_srcs
        toc/AdoptVisitor.cc
        toc/AdoptVisitor.h
        toc/BTreeIndex.cc
        toc/BTreeIndex.h
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
        toc/TocCommon.cc
        toc/TocCommon.h
        toc/TocCatalogue.cc
        toc/TocCatalogue.h
        toc/TocCatalogueReader.cc
        toc/TocCatalogueReader.h
        toc/TocCatalogueWriter.cc
        toc/TocCatalogueWriter.h
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
        toc/TocSerialisationVersion.cc
        toc/TocSerialisationVersion.h
        toc/TocWipeVisitor.cc
        toc/TocWipeVisitor.h
        toc/TocMoveVisitor.cc
        toc/TocMoveVisitor.h
        toc/TocRecord.cc
        toc/TocRecord.h
        toc/TocStats.cc
        toc/TocStats.h
        toc/TocStore.cc
        toc/TocStore.h
        toc/TocEngine.cc
        toc/TocEngine.h
        )

    list( APPEND fdb5_tools
        fdb-wipe
        fdb-stats
        fdb-purge
        fdb-dump-toc
        fdb-dump-index
        fdb-reconsolidate-toc
        fdb-move )
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

if( HAVE_RADOSFDB )
    list( APPEND fdb5_srcs
        rados/RadosFieldLocation.cc
        rados/RadosFieldLocation.h
        rados/RadosStore.cc
        rados/RadosStore.h
    )
endif()

ecbuild_add_library(

    TARGET  fdb5
    
    TYPE SHARED # Library needs to be shared for dynamic factory self registration

    SOURCES
        ${fdb5_srcs}

    INSTALL_HEADERS LISTED

    HEADER_DESTINATION
        ${INSTALL_INCLUDE_DIR}/fdb5

    PUBLIC_INCLUDES
        $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/src>
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>

    PUBLIC_LIBS
        metkit
        eckit
        eckit_option
        eckit_mpi

    PRIVATE_INCLUDES
        "${PMEM_INCLUDE_DIRS}"
        "${LUSTREAPI_INCLUDE_DIRS}"

    PRIVATE_LIBS
        ${grib_handling_pkg}
        ${PMEM_LIBRARIES}
        ${LUSTREAPI_LIBRARIES}
)

if(HAVE_FDB_BUILD_TOOLS)

  list( APPEND fdb5_scripts fdb fdb-which )

  foreach( _script ${fdb5_scripts} )
    configure_file(scripts/${_script}.in ${CMAKE_BINARY_DIR}/bin/${_script} @ONLY)
    install(PROGRAMS ${CMAKE_BINARY_DIR}/bin/${_script} DESTINATION ${INSTALL_BIN_DIR} )
  endforeach()
endif()

list( APPEND fdb5_tools
    fdb-write
    fdb-copy
    fdb-dump
    fdb-list
    fdb-read
    fdb-schema
    fdb-where
    fdb-info
    fdb-status
    fdb-lock
    fdb-unlock
)

if ( HAVE_GRIB )
    list( APPEND fdb5_tools
        fdb-hammer
        fdb-patch
    )
endif()

if ( HAVE_TOCFDB )
    list(APPEND fdb5_tools
        fdb-root
        fdb-overlay
        fdb-hide
    )
endif()

foreach( _tool ${fdb5_tools} )

  ecbuild_add_executable( TARGET    ${_tool}
                          CONDITION HAVE_FDB_BUILD_TOOLS
                          SOURCES   tools/${_tool}.cc
                          INCLUDES  ${ECCODES_INCLUDE_DIRS}  # Please don't remove me, I am needed
                          LIBS      PRIVATE fdb5 )

endforeach()

if (eckit_HAVE_MPI)
    target_link_libraries( fdb-move PRIVATE eckit_mpi )
endif()

ecbuild_add_executable( TARGET    grib2fdb5
                        CONDITION HAVE_FDB_BUILD_TOOLS
                        SOURCES   compat/grib2fdb5.cc
                        INCLUDES  ${ECCODES_INCLUDE_DIRS} # Please don't remove me, I am needed
                        LIBS      fdb5 )

ecbuild_add_executable(
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_FDB_REMOTE AND eckit_HAVE_ECKIT_CMD
        TARGET fdb-monitor
        SOURCES remote/fdb-monitor.cc
        LIBS eckit eckit_cmd )

ecbuild_add_executable(
        CONDITION HAVE_FDB_BUILD_TOOLS AND HAVE_FDB_REMOTE
        TARGET fdb-server
        SOURCES
            remote/fdb-server.cc
        LIBS fdb5 )

if ( HAVE_FDB_BUILD_TOOLS )
    target_sources( fdb-lock PRIVATE tools/FDBLock.cc tools/FDBLock.h )
    target_sources( fdb-unlock PRIVATE tools/FDBLock.cc tools/FDBLock.h )
endif()
