#pragma once
#include <unordered_map>
#include <string>
#include <vector>

class MarsGribMap {
public:
    // Translate MARS → GRIB keys (returns vector in case of 1:N mapping)
    static std::vector<std::string> translate(const std::string& marsKey) {
        auto it = mapping().find(marsKey);
        if (it != mapping().end()) {
            return it->second;
        }
        return {marsKey};
    }

    //access the full mapping
    static const std::unordered_map<std::string, std::vector<std::string>>& all() {
        return mapping();
    }

private:

static const std::unordered_map<std::string, std::vector<std::string>>& mapping() {
        static const std::unordered_map<std::string, std::vector<std::string>> m = {
            {"class",   {"marsClass"}},
            {"expver",  {"experimentVersionNumber"}},
            {"date",    {"year", "month", "day"}},
            {"time",    {"hour", "minute", "second"}}
        };
        return m;
    }
};

