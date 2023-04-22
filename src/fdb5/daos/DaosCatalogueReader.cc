/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <algorithm>

// #include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosName.h"

// #include "fdb5/toc/TocFieldLocation.h"
#include "fdb5/daos/DaosIndex.h"
#include "fdb5/daos/DaosCatalogueReader.h"
// #include "fdb5/toc/TocIndex.h"
// #include "fdb5/toc/TocStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosCatalogueReader::DaosCatalogueReader(const Key& key, const fdb5::Config& config) :
    DaosCatalogue(key, config) {

    // loadIndexesAndRemap();

    // fdb5::DaosKeyValueName main_kv_name{pool_, root_cont_, main_kv_};
    // ASSERT(main_kv_name.exists());

    // fdb5::DaosKeyValueName catalogue_kv_name{pool_, db_cont_, catalogue_kv_};
    // ASSERT(catalogue_kv_name.exists());

    /// @todo: schema is being loaded at DaosCatalogueWriter creation for write, but being loaded
    ///        at DaosCatalogueReader::open for read. Is this OK?

}

DaosCatalogueReader::DaosCatalogueReader(const eckit::URI& uri, const fdb5::Config& config) :
    DaosCatalogue(uri, ControlIdentifiers{}, config) {

    NOTIMP;
    // loadIndexesAndRemap();

}

// DaosCatalogueReader::~DaosCatalogueReader() {
//     eckit::Log::debug<LibFdb5>() << "Closing DB " << *dynamic_cast<DaosCatalogue*>(this) << std::endl;
// }

// void TocCatalogueReader::loadIndexesAndRemap() {
//     std::vector<Key> remapKeys;
//     std::vector<Index> indexes = loadIndexes(false, nullptr, nullptr, &remapKeys);

//     ASSERT(remapKeys.size() == indexes.size());
//     indexes_.reserve(remapKeys.size());
//     for (size_t i = 0; i < remapKeys.size(); ++i) {
//         indexes_.emplace_back(indexes[i], remapKeys[i]);
//     }
// }

bool DaosCatalogueReader::selectIndex(const Key &key) {

    if (currentIndexKey_ == key) {
        return true;
    }

    /// @todo: shouldn't this be set only if found a matching index?
    currentIndexKey_ = key;

    if (indexes_.find(key) == indexes_.end()) {

        fdb5::DaosKeyValueName catalogue_kv{pool_, db_cont_, catalogue_kv_};

        if (!catalogue_kv.has(key.valuesToString())) return false;

        fdb5::DaosSession s{};
        fdb5::DaosKeyValue catalogue_kv_obj{s, catalogue_kv};

        std::vector<char> n(100);
        catalogue_kv_obj.get(key.valuesToString(), &n[0], n.size());
        /// @todo: why is there a failure if using std::string(n.begin(), n.end())?
        fdb5::DaosKeyValueName index_kv{eckit::URI{std::string{&n[0]}}};

        indexes_[key] = Index(new fdb5::DaosIndex(key, index_kv));

    }

    current_ = indexes_[key];

    return true;

    // currentIndexKey_ = key;

    // for (auto idx = matching_.begin(); idx != matching_.end(); ++idx) {
    //     idx->first.close();
    // }

    // matching_.clear();


    // for (auto idx = indexes_.begin(); idx != indexes_.end(); ++idx) {
    //     if (idx->first.key() == key) {
    //         matching_.push_back(*idx);
    //     }
    // }

    // eckit::Log::debug<LibFdb5>() << "TocCatalogueReader::selectIndex " << key << ", found "
    //                             << matching_.size() << " matche(s)" << std::endl;

    // return (matching_.size() != 0);
}

void DaosCatalogueReader::deselectIndex() {

    NOTIMP; //< should not be called
    
}

bool DaosCatalogueReader::open() {

    if (!DaosCatalogue::exists()) {
        return false;
    }

    DaosCatalogue::loadSchema();
    return true;

}

void DaosCatalogueReader::axis(const std::string &keyword, eckit::StringSet &s) const {

    fdb5::DaosSession session{};
    /// @todo: take oclass from config
    fdb5::DaosKeyValueOID oid{currentIndexKey_.valuesToString() + std::string{"."} + keyword, OC_S1};
    fdb5::DaosKeyValueName n{pool_, db_cont_, oid};
    fdb5::DaosKeyValue kv{session, n};

    for (auto& k : kv.keys())
        s.insert(k);

}

// void TocCatalogueReader::close() {
//     for (auto m = matching_.begin(); m != matching_.end(); ++m) {
//         m->first.close();
//     }
// }

bool DaosCatalogueReader::retrieve(const Key& key, Field& field) const {

    eckit::Log::debug<LibFdb5>() << "Trying to retrieve key " << key << std::endl;
    eckit::Log::debug<LibFdb5>() << "Scanning index " << current_.location() << std::endl;

    /// @todo: should axes really be visited before querying index?
    ///        querying axes inflicts unnecessary IOPS but may reduce contention on index KV
    if (!current_.mayContain(key)) return false;

    return current_.get(key, fdb5::Key(), field);

    // for (auto m = matching_.begin(); m != matching_.end(); ++m) {
    //     const Index& idx(m->first);
    //     Key remapKey = m->second;

    //     if (idx.mayContain(key)) {
    //         const_cast<Index&>(idx).open();
    //         if (idx.get(key, remapKey, field)) {
    //             return true;
    //         }
    //     }
    // }

    // return false;

}

// void TocCatalogueReader::print(std::ostream &out) const {
//     out << "TocCatalogueReader(" << directory() << ")";
// }

// std::vector<Index> TocCatalogueReader::indexes(bool sorted) const {

//     std::vector<Index> returnedIndexes;
//     returnedIndexes.reserve(indexes_.size());
//     for (auto idx = indexes_.begin(); idx != indexes_.end(); ++idx) {
//         returnedIndexes.emplace_back(idx->first);
//     }

//     // If required, sort the indexes by file, and location within the file, for efficient iteration.
//     if (sorted) {
//         std::sort(returnedIndexes.begin(), returnedIndexes.end(), TocIndexFileSort());
//     }

//     return returnedIndexes;
// }

static fdb5::CatalogueBuilder<fdb5::DaosCatalogueReader> builder("daos.reader");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
