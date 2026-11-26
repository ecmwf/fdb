// fdb library metadata + runtime initialisation bridge.
#pragma once

#include "rust/cxx.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

class Library {
public:

    /// Initialise the FDB library (sets up `eckit::Main`). Idempotent.
    static void initialise();

    /// Get the FDB library version string.
    static rust::String version();

    /// Get the FDB git SHA1 hash.
    static rust::String git_sha1();
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
