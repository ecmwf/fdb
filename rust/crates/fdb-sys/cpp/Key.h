// fdb Key bridge — converters between the FFI `KeyData` struct and the
// underlying `fdb5::Key`.
#pragma once

#include "Types.h"
#include "fdb5/database/Key.h"

#include "rust/cxx.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

/// Conversion helpers between the FFI `KeyData` shared struct and the C++
/// `fdb5::Key` type.
class Key {
public:

    /// Build an `fdb5::Key` from the FFI `KeyData` carrier.
    static fdb5::Key from_data(const KeyData& data);

    /// Render an `fdb5::Key` as a `Vec<KeyValue>` for return over the bridge.
    static rust::Vec<KeyValue> to_data(const fdb5::Key& key);
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
