#include "fdb5/api/helpers/ListIterator.h"

#include <unordered_map>

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
        if (deduplicate_ || onlyDuplicates_) {
            if (seenKeys_.tryInsert(tmp.keys())) {
                if (onlyDuplicates_) continue; // haven't seen this key before, skip it
            } else {
                if (deduplicate_) continue; // already seen this key, skip it
            }
        }
        std::swap(elem, tmp);
        return true;
    }
    return false;
}

std::pair<size_t, eckit::Length> ListIterator::dumpCompact(std::ostream& out) {

    std::map<std::string,
             std::map<std::string, std::pair<metkit::mars::MarsRequest, std::unordered_map<Key, eckit::Length>>>>
        requests;

    size_t fields{0};
    eckit::Length length{0};

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
            std::map<std::string, std::pair<metkit::mars::MarsRequest, std::unordered_map<Key, eckit::Length>>> leaves;
            leaves.emplace(signature, std::make_pair(keys[2].request(),
                                                     std::unordered_map<Key, eckit::Length>{{keys[2], elem.length()}}));
            requests.emplace(treeAxes, leaves);
        }
        else {
            auto h = it->second.find(signature);
            if (h != it->second.end()) {  // the hypercube request is already there... adding the 3rd level key
                h->second.first.merge(keys[2].request());
                h->second.second.emplace(keys[2], elem.length());
            }
            else {
                it->second.emplace(signature, std::make_pair(keys[2].request(), std::unordered_map<Key, eckit::Length>{
                                                                                    {keys[2], elem.length()}}));
            }
        }
    }  // while

    for (const auto& tree : requests) {
        for (const auto& leaf : tree.second) {
            fields += leaf.second.second.size();
            for (auto [k, l] : leaf.second.second) {
                length += l;
            }
            metkit::hypercube::HyperCube h{leaf.second.first};
            if (h.size() == leaf.second.second.size()) {
                out << tree.first << ",";
                leaf.second.first.dump(out, "", "", false);
                out << std::endl;
            }
            else {
                for (const auto& [k, l] : leaf.second.second) {
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
    return std::make_pair(fields, length);
}

//----------------------------------------------------------------------------------------------------------------------

bool KeyStore::tryInsert(const std::array<Key, 3>& keyParts) {
    // Level 1 and 2, may or may not exist. But it doesn't matter for the insert
    auto& level1 = fingerprints_[keyParts[0].valuesToString()];
    auto& level2 = level1[keyParts[1].valuesToString()];

    // And then for the third level, we try and insert. And if it does not
    // already exist, that is great!

    auto [_, ins] = level2.insert(keyParts[2].valuesToString());
    return ins;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
