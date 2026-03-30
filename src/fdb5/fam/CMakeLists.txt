# FamFieldLocation is independent of FAMFDB.
# We compile it even if FAMFDB is not available for client/server builds.
list( APPEND fdb5_srcs
    fam/FamFieldLocation.cc
    fam/FamFieldLocation.h
)

if( HAVE_FAMFDB )
    list( APPEND fdb5_srcs
        fam/FamCommon.cc
        fam/FamCommon.h
        fam/FamStore.cc
        fam/FamStore.h
        fam/FamIndexLocation.cc
        fam/FamIndexLocation.h
        fam/FamIndex.cc
        fam/FamIndex.h
        fam/FamCatalogue.cc
        fam/FamCatalogue.h
        fam/FamCatalogueReader.cc
        fam/FamCatalogueReader.h
        fam/FamCatalogueWriter.cc
        fam/FamCatalogueWriter.h
        fam/FamEngine.cc
        fam/FamEngine.h
    )
endif()

set( fdb5_srcs  ${fdb5_srcs} PARENT_SCOPE )
