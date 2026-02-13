/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "fdb5/tools/compare/mars/Compare.h"
#include <unordered_map>

namespace compare::mars {

Result compareMarsKeys(const DataIndex& ref, const DataIndex& test, const Options& opts) {
    eckit::Log::info() << "[LOG] Checking MARS Keys\n";
    Result res;

    auto printMismatch = [&](const std::map<std::string, std::string>& keymap, const std::string& logDir) {
        eckit::Log::info() << "[MARS KEYS COMPARE] MISMATCH (" << logDir << "): ";
        for (const auto& kv : keymap)
            eckit::Log::info() << kv.first << "=" << kv.second << " ";
        eckit::Log::info() << "\n";
        return true;
    };

    int mismatches = 0;
    // Copy map to erase keys
    std::unordered_set<fdb5::Key> refRemaining;
    for (const auto& p : ref) {
        refRemaining.insert(p.first);
    }
    for (const auto& it : test) {
        const auto& keymap = it.first;
        bool found         = false;

        if (opts.marsReqDiff.empty()) {
            found = ref.find(keymap) != ref.end();

            if (found) {
                refRemaining.erase(keymap);
            }
        }
        else {
            auto expected = applyKeyDiff(keymap, opts.marsReqDiff, true);
            found         = ref.find(expected) != ref.end();
            if (found) {
                refRemaining.erase(expected);
            }
        }

        if (!found) {
            ++mismatches;
            eckit::Log::info() << "[MARS KEYS COMPARE] MISMATCH (Test->Reference): ";
            for (const auto& kv : keymap)
                eckit::Log::info() << kv.first << "=" << kv.second << " ";
            eckit::Log::info() << "\n";
        }
    }

    // All are identical
    if (mismatches == 0 && test.size() == ref.size()) {
        eckit::Log::info() << "[MARS KEYS COMPARE] SUCCESS\n";
        res.match = true;
        return res;
    }

    // Print remaining
    for (const auto& it : refRemaining) {
        eckit::Log::info() << "[MARS KEYS COMPARE] MISMATCH (Reference->Test): ";
        eckit::Log::info() << it;
        eckit::Log::info() << "\n";
    }
    res.match = false;
    return res;
}


}  // namespace compare::mars
