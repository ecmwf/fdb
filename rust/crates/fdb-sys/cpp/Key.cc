// fdb Key bridge — implementation.

#include "fdb_exceptions.h"

#include "Key.h"
#include "fdb-sys/src/lib.rs.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

fdb5::Key Key::from_data(const KeyData& data) {
    fdb5::Key key;
    for (const auto& entry : data.entries) {
        key.set(std::string(entry.key), std::string(entry.value));
    }
    return key;
}

rust::Vec<KeyValue> Key::to_data(const fdb5::Key& key) {
    rust::Vec<KeyValue> result;
    for (const auto& [k, v] : key) {
        KeyValue kv;
        kv.key = rust::String(k);
        kv.value = rust::String(v);
        result.push_back(std::move(kv));
    }
    return result;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
