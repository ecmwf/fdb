if( HAVE_S3FDB )

    list( APPEND fdb5_srcs
        s3/S3Common.cc
        s3/S3Common.h
        s3/S3FieldLocation.cc
        s3/S3FieldLocation.h
        s3/S3Root.cc
        s3/S3Root.h
        s3/S3RootManager.cc
        s3/S3RootManager.h
        s3/S3Store.cc
        s3/S3Store.h
    )

    set( fdb5_srcs ${fdb5_srcs} PARENT_SCOPE )

endif()
