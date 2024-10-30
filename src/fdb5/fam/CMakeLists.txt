if( HAVE_FAMFDB )
    list( APPEND fdb5_srcs
        fam/FamCatalogue.h
        fam/FamCatalogue.cc
        fam/FamCommon.cc
        fam/FamCommon.h
        fam/FamEngine.cc
        fam/FamEngine.h
        fam/FamFieldLocation.cc
        fam/FamFieldLocation.h
        fam/FamIndex.cc
        fam/FamIndex.h
        fam/FamIndexLocation.cc
        fam/FamIndexLocation.h
        fam/FamRecord.cc
        fam/FamRecord.h
        fam/FamRecordVersion.cc
        fam/FamRecordVersion.h
        fam/FamIndex.cc
        fam/FamIndex.h
        fam/FamStore.cc
        fam/FamStore.h
    )
    set( fdb5_srcs  ${fdb5_srcs} PARENT_SCOPE )
endif()
