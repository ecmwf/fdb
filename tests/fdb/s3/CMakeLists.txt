if ( fdb5_HAVE_S3FDB )

    set( S3_TEST_ROOT       "fdb_s3_test_root" )
    set( S3_TEST_HOST       "minio" )
    set( S3_TEST_PORT       "9000" )
    set( S3_TEST_ENDPOINT   ${S3_TEST_HOST}:${S3_TEST_PORT} )
    set( S3_TEST_REGION     "eu-central-1" )
    set( S3_TEST_BUCKET     "fdb5-test-s3-bucket" )
    set( S3_TEST_OBJECT     "fdb5-test-s3-object" )

    configure_file( test_s3_config.h.in test_s3_config.h @ONLY )

    configure_file( config.yaml.in config.yaml @ONLY )
    configure_file( S3Config.yaml.in S3Config.yaml @ONLY )
    configure_file( S3Credentials.yaml.in S3Credentials.yaml @ONLY )
    configure_file( fdb_s3_test_schema schema COPYONLY )

    set( test_srcs store )

    foreach( _test ${test_srcs} )

      ecbuild_add_test( TARGET      test_fdb5_s3_${_test}
                        SOURCES     test_${_test}.cc
                        INCLUDES    ${CMAKE_CURRENT_BINARY_DIR}
                        LIBS        fdb5
                        ENVIRONMENT "${_test_environment}"
      )

    endforeach()

endif( fdb5_HAVE_S3FDB )
