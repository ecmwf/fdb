ecbuild_add_option( FEATURE WARNINGS
                    DEFAULT ON
                    DESCRIPTION "Add warnings to compiler" )

# activate warnings, ecbuild macros check the compiler recognises the options
if(HAVE_WARNINGS)

  ecbuild_add_cxx_flags("-Wall" NO_FAIL)
  ecbuild_add_cxx_flags("-Wextra" NO_FAIL)

  ecbuild_add_cxx_flags("-Wno-unused-parameter" NO_FAIL)
  ecbuild_add_cxx_flags("-Wno-unused-variable" NO_FAIL)
  ecbuild_add_cxx_flags("-Wno-sign-compare" NO_FAIL)

endif()
