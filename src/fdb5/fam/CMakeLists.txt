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
    )
endif()

set( fdb5_srcs  ${fdb5_srcs} PARENT_SCOPE )
