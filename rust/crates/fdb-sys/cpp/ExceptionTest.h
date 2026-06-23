// fdb exception-handling smoke tests — used by `#[cfg(test)]` in `lib.rs` to
// verify the cxx `trycatch` shim translates each thrown type correctly.
#pragma once

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

class ExceptionTest {
public:

    static void throw_eckit_exception();
    static void throw_eckit_serious_bug();
    static void throw_eckit_user_error();
    static void throw_std_exception();
    static void throw_int();
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
