#include "fdb5/api/helpers/ListIterator.h"

#include "metkit/hypercube/HyperCube.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static std::string keySignature(const fdb5::Key& key) {
    std::string signature;
    std::string separator;
    for (auto&& k : key.keys()) {
        signature += separator + k;
        separator = ":";
    }
    return signature;
}

//----------------------------------------------------------------------------------------------------------------------

bool ListIterator::next(ListElement& elem) {
    ListElement tmp;
    while (APIIterator<ListElement>::next(tmp)) {
        if (deduplicate_) {
            if (const auto [iter, success] = seenKeys_.emplace(tmp.combinedKey()); !success) {
                continue;
            }
        }
        std::swap(elem, tmp);
        return true;
    }
    return false;
}

void ListIterator::dumpCompact(std::ostream& out) {

    std::map<std::string, std::map<std::string, std::pair<metkit::mars::MarsRequest, std::unordered_set<Key>>>>
        requests;

    ListElement elem;
    while (next(elem)) {

        const auto& keys = elem.keys();
        ASSERT(keys.size() == 3);

        std::string treeAxes = keys[0];
        treeAxes += ",";
        treeAxes += keys[1];

        std::string signature = keySignature(keys[2]);  // i.e. step:levelist:param

        auto it = requests.find(treeAxes);
        if (it == requests.end()) {
            std::map<std::string, std::pair<metkit::mars::MarsRequest, std::unordered_set<Key>>> leaves;
            leaves.emplace(signature, std::make_pair(keys[2].request(), std::unordered_set<Key>{keys[2]}));
            requests.emplace(treeAxes, leaves);
        }
        else {
            auto h = it->second.find(signature);
            if (h != it->second.end()) {  // the hypercube request is already there... adding the 3rd level key
                h->second.first.merge(keys[2].request());
                h->second.second.insert(keys[2]);
            }
            else {
                it->second.emplace(signature, std::make_pair(keys[2].request(), std::unordered_set<Key>{keys[2]}));
            }
        }
    }  // while

    for (const auto& tree : requests) {
        for (const auto& leaf : tree.second) {
            metkit::hypercube::HyperCube h{leaf.second.first};
            if (h.size() == leaf.second.second.size()) {
                out << tree.first << ",";
                leaf.second.first.dump(out, "", "", false);
                out << std::endl;
            }
            else {
                for (const auto& k : leaf.second.second) {
                    h.clear(k.request());
                }
                for (const auto& r : h.requests()) {
                    out << tree.first << ",";
                    r.dump(out, "", "", false);
                    out << std::endl;
                }
            }
        }
    }
}

}  // namespace fdb5
