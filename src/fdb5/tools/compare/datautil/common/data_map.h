#pragma once
#include <map>
#include <string>
#include <functional>

namespace compare::common {

// struct DataLocation {
//     std::string path;
//     long long   offset = 0;
//     long long   length = 0;
// }; -> move towards struct
using DataLocation = std::tuple<std::string, long long, long long>;

using KeyMap = std::map<std::string, std::string>;

struct MapHash {
    size_t operator()(const KeyMap& m) const noexcept {
        // order is stable in std::map; fold key=value pairs
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

} 