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
    api/helpers/AxesIterator.cc
    api/helpers/AxesIterator.h
    api/helpers/ControlIterator.cc
    api/helpers/ControlIterator.h
    api/helpers/FDBToolRequest.cc
    api/helpers/FDBToolRequest.h
    api/helpers/DumpIterator.h
    api/helpers/ListElement.cc
    api/helpers/ListElement.h
    api/helpers/ListIterator.cc
    api/helpers/ListIterator.h
    api/helpers/LockIterator.h
    api/helpers/MoveIterator.h
    api/helpers/StatusIterator.h
    api/helpers/WipeIterator.cc
    api/helpers/WipeIterator.h
    api/helpers/PurgeIterator.h
    api/helpers/StatsIterator.cc
    api/helpers/StatsIterator.h
    api/helpers/Callback.h

    api/local/QueryVisitor.h
    api/local/QueueStringLogTarget.h
    api/local/ListVisitor.h
    api/local/AxesVisitor.cc
    api/local/AxesVisitor.h
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
    database/BaseKey.cc
    database/BaseKey.h
    database/Catalogue.cc
    database/Catalogue.h
    database/DatabaseNotFoundException.cc
    database/DatabaseNotFoundException.h
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
    database/Signature.h
    database/StatsReportVisitor.cc
    database/StatsReportVisitor.h
    database/Store.cc
    database/Store.h
    database/PurgeVisitor.cc
    database/PurgeVisitor.h
    database/WipeCoordinator.cc
    database/WipeCoordinator.h
    database/WipeState.cc
    database/WipeState.h
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
    database/ReindexVisitor.cc
    database/ReindexVisitor.h
    database/Reindexer.cc
    database/Reindexer.h
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
    io/FieldHandle.cc
    io/FieldHandle.h
    rules/MatchAlways.cc
    rules/MatchAlways.h
    rules/MatchAny.cc
    rules/MatchAny.h
    rules/Matcher.cc
    rules/Matcher.h
    rules/MatchHidden.cc
    rules/MatchHidden.h
    rules/ExcludeAll.cc
    rules/ExcludeAll.h
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
    rules/SelectMatcher.cc
    rules/SelectMatcher.h
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
    types/TypeMonthOfDate.cc
    types/TypeMonthOfDate.h
    types/TypeYear.cc
    types/TypeYear.h
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

        remote/Connection.h
        remote/Connection.cc
        remote/RemoteFieldLocation.h
        remote/RemoteFieldLocation.cc
        remote/Messages.h
        remote/Messages.cc
        remote/FdbServer.h
        remote/FdbServer.cc
        remote/RemoteConfiguration.h
        remote/RemoteConfiguration.cc

        remote/client/Client.h
        remote/client/Client.cc
        remote/client/ClientConnection.h
        remote/client/ClientConnection.cc
        remote/client/ClientConnectionRouter.h
        remote/client/ClientConnectionRouter.cc
        remote/client/RemoteStore.cc
        remote/client/RemoteStore.h
        remote/client/RemoteCatalogue.h
        remote/client/RemoteCatalogue.cc
        remote/client/ReadLimiter.h
        remote/client/ReadLimiter.cc

        remote/server/AvailablePortList.cc
        remote/server/AvailablePortList.h
        remote/server/CatalogueHandler.h
        remote/server/CatalogueHandler.cc
        remote/server/StoreHandler.h
        remote/server/StoreHandler.cc
        remote/server/ServerConnection.h
        remote/server/ServerConnection.cc
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
        fdb-move
        fdb-reconsolidate-toc )
endif()

if( HAVE_RADOSFDB )
    list( APPEND fdb5_srcs
        rados/RadosFieldLocation.cc
        rados/RadosFieldLocation.h
        rados/RadosStore.cc
        rados/RadosStore.h
    )
endif()

if( HAVE_DAOSFDB )
    list( APPEND fdb5_srcs
        daos/DaosOID.cc
        daos/DaosOID.h
        daos/DaosName.cc
        daos/DaosName.h
        daos/DaosObject.cc
        daos/DaosObject.h
        daos/DaosContainer.cc
        daos/DaosContainer.h
        daos/DaosPool.cc
        daos/DaosPool.h
        daos/DaosSession.cc
        daos/DaosSession.h
        daos/DaosArrayHandle.cc
        daos/DaosArrayHandle.h
        daos/DaosKeyValueHandle.cc
        daos/DaosKeyValueHandle.h
        daos/DaosStore.h
        daos/DaosStore.cc
        daos/DaosFieldLocation.h
        daos/DaosFieldLocation.cc
        daos/DaosException.h
        daos/DaosException.cc
        daos/DaosEngine.h
        daos/DaosEngine.cc
        daos/DaosCatalogue.h
        daos/DaosCatalogue.cc
        daos/DaosCatalogueWriter.h
        daos/DaosCatalogueWriter.cc
        daos/DaosCatalogueReader.h
        daos/DaosCatalogueReader.cc
        daos/DaosIndex.h
        daos/DaosIndex.cc
        daos/DaosIndexLocation.h
        daos/DaosIndexLocation.cc
        daos/DaosCommon.h
        daos/DaosCommon.cc
        daos/DaosArrayPartHandle.h
        daos/DaosArrayPartHandle.cc
        daos/DaosLazyFieldLocation.h
        daos/DaosLazyFieldLocation.cc
        daos/UUID.h
        daos/UUID.cc
    )
else()
    set( DAOS_LIBRARIES "" )
    set( DAOS_INCLUDE_DIRS "" )
    set( UUID_LIBRARIES "" )
    set( UUID_INCLUDE_DIRS "" )
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
        "${UUID_LIBRARIES}"

    PRIVATE_INCLUDES
        "${LUSTREAPI_INCLUDE_DIRS}"
        "${DAOS_INCLUDE_DIRS}"
        "${DAOS_TESTS_INCLUDE_DIRS}"

    PRIVATE_LIBS
        ${LUSTREAPI_LIBRARIES}
        "${DAOS_LIBRARIES}"
        "${DAOS_TESTS_LIBRARIES}"
        $<$<BOOL:${HAVE_GRIB}>:eccodes>
)

if(HAVE_FDB_BUILD_TOOLS)

  list( APPEND fdb5_scripts fdb fdb-which )

  foreach( _script ${fdb5_scripts} )
    configure_file(scripts/${_script}.in ${CMAKE_BINARY_DIR}/bin/${_script} @ONLY)
    install(PROGRAMS ${CMAKE_BINARY_DIR}/bin/${_script} DESTINATION ${INSTALL_BIN_DIR} )
  endforeach()
endif()

list( APPEND fdb5_tools
    fdb-axes
    fdb-write
    fdb-copy
    fdb-dump
    fdb-list
    fdb-read
    fdb-schema
    fdb-where
    fdb-info
    fdb-inspect
    fdb-status
    fdb-lock
    fdb-unlock
    fdb-reindex
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
                          LIBS      fdb5 )

endforeach()

if (HAVE_FDB_BUILD_TOOLS)
    target_link_libraries( fdb-move eckit_distributed )
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
