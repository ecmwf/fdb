#pragma once
#include <unordered_map>
#include <string>

inline const std::string& marsToGrib(const std::string& marsKey) {
    static const std::unordered_map<std::string, std::string> mapping = {
        {"class",   "marsClass"},
        {"expver",  "experimentVersionNumber"},
        {"date",    "year, month, day"},
        {"time",    "hour, minute, second"}
        // add more mappings as needed
    };

    auto it = mapping.find(marsKey);
    if (it != mapping.end()) {
        return it->second;
    }
    return marsKey; // fallback: return same key if no mapping found
}