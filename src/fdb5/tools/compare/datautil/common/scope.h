#pragma once
#include <map>
#include <string>
#include <utility>

namespace compare {

enum class Scope {Mars, HeaderOnly, All };

inline Scope parseScope(const std::string& s) {
    if (s == "header-only") {
        return Scope::HeaderOnly;
    } else if (s == "all") {
        return Scope::All;
    } else {
        // default or "mars"
        return Scope::Mars;
    }
}


struct Options {
    Scope scope = Scope::Mars;

    // For MARS comparison:
    std::map<std::string, std::string> ignoreMarsKeys;
    std::map<std::string, std::pair<std::string,std::string>> marsReqDiff;

    // Optional explicit requests (stringified form you already accept)
    std::string referenceRequest;
    std::string testRequest;
};

struct Result {
    bool ok = true;             // overall success
    bool stoppedEarly = false;  // e.g. MARS mismatch aborts deeper compares
};

} 