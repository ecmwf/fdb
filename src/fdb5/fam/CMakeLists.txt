list( APPEND fdb5_srcs
    fam/FamFieldLocation.h
    fam/FamFieldLocation.cc
)

if( HAVE_FAMFDB )
    list( APPEND fdb5_srcs
        fam/FamCommon.h
        fam/FamCommon.cc
        fam/FamStore.h
        fam/FamStore.cc
        fam/FamIndexLocation.h
        fam/FamIndexLocation.cc
        fam/FamIndex.h
        fam/FamIndex.cc
        fam/FamCatalogue.h
        fam/FamCatalogue.cc
        fam/FamCatalogueReader.h
        fam/FamCatalogueReader.cc
        fam/FamCatalogueWriter.h
        fam/FamCatalogueWriter.cc
        fam/FamEngine.h
        fam/FamEngine.cc
    )
endif()

set( fdb5_srcs  ${fdb5_srcs} PARENT_SCOPE )
