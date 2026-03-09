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

Result compareMarsKeys(const DataIndex& i1, const DataIndex& i2, const Options& opts) {
    eckit::Log::info() << "[LOG] Checking MARS Keys\n";
    Result res;

    auto printMismatch = [&](const std::map<std::string, std::string>& keymap, const std::string& logDir) {
        eckit::Log::info() << "[MARS KEYS COMPARE] MISMATCH (" << logDir << "): ";
        for (const auto& kv : keymap) {
            eckit::Log::info() << kv.first << "=" << kv.second << " ";
        }
        eckit::Log::info() << "\n";
        return true;
    };

    int mismatches = 0;
    // Copy map to erase keys
    std::unordered_set<fdb5::Key> i1Remaining;
    for (const auto& p : i1) {
        i1Remaining.insert(p.first);
    }
    for (const auto& it : i2) {
        const auto& keymap = it.first;
        bool found = false;

        if (opts.marsReqDiff.empty()) {
            found = i1.find(keymap) != i1.end();

            if (found) {
                i1Remaining.erase(keymap);
            }
        }
        else {
            auto expected = applyKeyDiff(keymap, opts.marsReqDiff, true);
            found = i1.find(expected) != i1.end();
            if (found) {
                i1Remaining.erase(expected);
            }
        }

        if (!found) {
            ++mismatches;
            eckit::Log::info() << "[MARS KEYS COMPARE] MISMATCH (Test->Reference): ";
            for (const auto& kv : keymap) {
                eckit::Log::info() << kv.first << "=" << kv.second << " ";
            }
            eckit::Log::info() << "\n";
        }
    }

    // All are identical
    if (mismatches == 0 && i2.size() == i1.size()) {
        eckit::Log::info() << "[MARS KEYS COMPARE] SUCCESS\n";
        res.match = true;
        return res;
    }

    // Print remaining
    for (const auto& it : i1Remaining) {
        eckit::Log::info() << "[MARS KEYS COMPARE] MISMATCH (Reference->Test): ";
        eckit::Log::info() << it;
        eckit::Log::info() << "\n";
    }
    res.match = false;
    return res;
}


}  // namespace compare::mars
