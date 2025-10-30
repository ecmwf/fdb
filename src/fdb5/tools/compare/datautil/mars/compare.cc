#include "datautil/mars/compare.h"
// #include "datautil/commom/comparison_map.h"


#include <unordered_map>
#include <iostream>

namespace compare::mars {

using namespace compare::common;




static bool maps_equal_with_diff(const KeyMap& dst, const KeyMap& src,
                                 const std::map<std::string,std::pair<std::string,std::string>>& diff) {
    if (dst.size() != src.size()) return false;

    for (const auto& [k, v_a] : dst) {
        auto it_b = src.find(k);
        if (it_b == src.end()) return false;  // key missing
        const std::string& v_b = it_b->second;

        auto it_diff = diff.find(k);
        if (it_diff != diff.end()) {
            // This key is allowed to diverge
            const auto& [expected_ref, expected_test] = it_diff->second;
            if (v_a == expected_ref && v_b == expected_test) {
                continue; // okay, matches allowed divergence
            } else {
                return false; // diverges in an unexpected way
            }
        } else {
            // This key must match exactly
            if (v_a != v_b) return false;
        }
    }
    return true;
}


static bool compareMarsKeys(const DataIndex& ref,
                            const DataIndex& test,
                            const std::map<std::string,std::pair<std::string,std::string>>& diff) {

    std::cout << "[LOG] Checking MARS Keys\n";
    int mismatches = 0;

    auto check = [&](const DataIndex& src, const DataIndex& dst, const char* dir, bool count) {
        for (const auto& it : src) {
            const auto& keymap = it.first;
            bool found = false;
            for (const auto& x : diff) {
        
                std::cout<< x.first<<".  "<<x.second.first << "  "<< x.second.second <<std::endl;
            }
            if (diff.empty()) {
                found = dst.find(keymap) != dst.end();
            } else {
                for (const auto& jt : dst) {
                    if (maps_equal_with_diff(jt.first, keymap, diff)) { found = true; break; }
                }
            }
            if (!found) {
                if (count) ++mismatches;
                std::cout << "[MARS KEYS COMPARE] MISMATCH (" << dir << "): ";
                for (const auto& kv : keymap) std::cout << kv.first << "=" << kv.second << " ";
                std::cout << "\n";
            }
        }
    };

    check(test, ref, "Test->Reference", true);

    if (mismatches == 0 && test.size() == ref.size()) {
        std::cout << "[MARS KEYS COMPARE] SUCCESS\n";
        return true;
    }

    check(ref, test, "Reference->Test", false);
    return false;
}



Result MarsComparator::compare(const Options& opts) {
    Result r;
    r.ok = compareMarsKeys(ref_, test_, opts.marsReqDiff);
    r.stoppedEarly = !r.ok;
    return r;
}

std::unique_ptr<IComparator> makeMarsComparator(const DataIndex& ref,
                                                const DataIndex& test) {
    return std::make_unique<MarsComparator>(ref, test);
}


} // namespace compare::mars
