#pragma once
#include <map>
#include <string>
#include <functional>
#include <iostream>
#include <unordered_map>

namespace compare::common {

struct DataLocation {
    std::string path;
    long long offset = 0;
    long long length = 0;
};

using KeyMap = std::map<std::string, std::string>;

struct MapHash {
    size_t operator()(const KeyMap& m) const noexcept {
        size_t h = 0;
        std::hash<std::string> hs;
        for (const auto& kv : m) {
            h ^= (hs(kv.first) * 1315423911u) ^ hs(kv.second);
        }
        return h;
    }
};

struct MapEqual {
    bool operator()(const KeyMap& a, const KeyMap& b) const noexcept {
        return a == b;
    }
};

using DataIndex = std::unordered_map<KeyMap, DataLocation, MapHash, MapEqual>;

/// Print a KeyMap
inline std::ostream& operator<<(std::ostream& os, const KeyMap& km) {
    os << "{";
    for (const auto& [k,v] : km) {
        os << k << "=" << v << ", ";
    }
    os << "}";
    return os;
}

/// Print a DataLocation
inline std::ostream& operator<<(std::ostream& os, const DataLocation& loc) {
    return os << "(" << loc.path << ", " << loc.offset << ", " << loc.length << ")";
}

/// Print a DataIndex
inline std::ostream& operator<<(std::ostream& os, const DataIndex& idx) {
    for (const auto& [km, loc] : idx) {
        os << "Key: " << km << " -> Value: " << loc << "\n";
    }
    return os;
}

} // namespace compare::common