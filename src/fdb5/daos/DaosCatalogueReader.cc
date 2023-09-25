/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/LibFdb5.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosName.h"

#include "fdb5/daos/DaosIndex.h"
#include "fdb5/daos/DaosCatalogueReader.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosCatalogueReader::DaosCatalogueReader(const Key& key, const fdb5::Config& config) :
    DaosCatalogue(key, config) {

    /// @todo: schema is being loaded at DaosCatalogueWriter creation for write, but being loaded
    ///        at DaosCatalogueReader::open for read. Is this OK?

}

DaosCatalogueReader::DaosCatalogueReader(const eckit::URI& uri, const fdb5::Config& config) :
    DaosCatalogue(uri, ControlIdentifiers{}, config) {}

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

        daos_size_t size{catalogue_kv_obj.size(key.valuesToString())};
        std::vector<char> n((long) size);
        catalogue_kv_obj.get(key.valuesToString(), &n[0], size);
        fdb5::DaosKeyValueName index_kv{eckit::URI{std::string{n.begin(), n.end()}}};

        indexes_[key] = Index(new fdb5::DaosIndex(key, index_kv));

    }

    current_ = indexes_[key];

    return true;

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

    // for (int i = 0; i <= 100; ++i)
    //     s.insert(std::to_string(i));

}

bool DaosCatalogueReader::retrieve(const Key& key, Field& field) const {

    eckit::Log::debug<LibFdb5>() << "Trying to retrieve key " << key << std::endl;
    eckit::Log::debug<LibFdb5>() << "Scanning index " << current_.location() << std::endl;

    /// @todo: should axes really be visited before querying index? I don't think so, because
    ///        querying axes inflicts unnecessary IOPS. But it may help reduce contention on 
    ///        index KV
    // if (!current_.mayContain(key)) return false;

    return current_.get(key, fdb5::Key(), field);

}

static fdb5::CatalogueBuilder<fdb5::DaosCatalogueReader> builder("daos.reader");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
