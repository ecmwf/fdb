#include "datautil/common/comparison_map.h"


namespace compare::common {

    static bool isSubset(const KeyMap& a, const KeyMap& b) {
        for (const auto& kv : a) {
            auto it = b.find(kv.first);
            if (it == b.end() || it->second != kv.second) return false;
        }
        return true;
    }



    void assemble_compare_map(fdb5::FDB& fdb,
                                DataIndex& out,
                                const fdb5::FDBToolRequest& req,
                                const KeyMap& ignore) {

        auto list = fdb.list(req);
        fdb5::ListElement elem;

        std::cout<<req<<std::endl;
        std::cout<<ignore<<std::endl;
        while (list.next(elem)) {
            KeyMap km;
            for (const auto& bit : elem.keys()) {
                auto d = bit.keyDict();
                km.insert(d.begin(), d.end());
            }
            if (ignore.empty() || !isSubset(ignore, km)) {
                out.emplace(km, DataLocation {
                    elem.location().uri().path(),
                    static_cast<long long>(elem.location().offset()),
                    static_cast<long long>(elem.location().length())
                });
            }
        }
        std::cout << "[LOG] FDB request: " << req << " resulted in " << out.size() << " entries.\n";
    }
}