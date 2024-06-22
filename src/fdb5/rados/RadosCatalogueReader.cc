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
#include "fdb5/rados/RadosIndex.h"
#include "fdb5/rados/RadosCatalogueReader.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// @note: as opposed to the TOC catalogue, the DAOS catalogue does not pre-load all indexes from storage.
///   Instead, it selects and loads only those indexes that are required to fulfil the request.

RadosCatalogueReader::RadosCatalogueReader(const Key& key, const fdb5::Config& config) :
    RadosCatalogue(key, config) {

    /// @todo: schema is being loaded at DaosCatalogueWriter creation for write, but being loaded
    ///        at DaosCatalogueReader::open for read. Is this OK?

}

RadosCatalogueReader::RadosCatalogueReader(const eckit::URI& uri, const fdb5::Config& config) :
    RadosCatalogue(uri, ControlIdentifiers{}, config) {}

bool RadosCatalogueReader::selectIndex(const Key &key) {

    if (currentIndexKey_ == key) {
        return true;
    }

    /// @todo: shouldn't this be set only if found a matching index?
    currentIndexKey_ = key;

    if (indexes_.find(key) == indexes_.end()) {

        /// @note: performed RPCs:
        /// - generate catalogue kv oid (daos_obj_generate_oid)
        /// - ensure catalogue kv exists (daos_kv_open)

        int idx_loc_max_len = 512;  /// @todo: take from config
        std::vector<char> n((long) idx_loc_max_len);
        long res;

        try {

            /// @note: performed RPCs:
            /// - retrieve index kv location from catalogue kv (daos_kv_get)
            res = db_kv_->get(key.valuesToString(), &n[0], idx_loc_max_len);

        } catch (eckit::RadosEntityNotFoundException& e) {

            /// @note: performed RPCs:
            /// - close catalogue kv (daos_obj_close)

            return false;

        }

        eckit::URI uri{std::string{n.begin(), std::next(n.begin(), res)}};
// #ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE
//         eckit::RadosPersistentKeyValue index_kv{uri, true};
// #elif fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH
//         eckit::RadosPersistentKeyValue index_kv{uri};
// #else
        eckit::RadosKeyValue index_kv{uri};
// #endif

        indexes_[key] = Index(new fdb5::RadosIndex(key, index_kv, true));

        /// @note: performed RPCs:
        /// - close catalogue kv (daos_obj_close)

    }

    current_ = indexes_[key];

    return true;

}

void RadosCatalogueReader::deselectIndex() {

    NOTIMP; //< should not be called
    
}

bool RadosCatalogueReader::open() {

    /// @note: performed RPCs:
    /// - daos_pool_connect
    /// - daos_cont_open
    /// - daos_obj_generate_oid
    /// - daos_kv_open
    if (!RadosCatalogue::exists()) {
        return false;
    }

    RadosCatalogue::loadSchema();
    return true;

}

bool RadosCatalogueReader::axis(const std::string &keyword, eckit::StringSet &s) const {

    bool found = false;
    if (current_.axes().has(keyword)) {
        found = true;
        const eckit::DenseSet<std::string>& a = current_.axes().values(keyword);
        s.insert(a.begin(), a.end());
    }
    return found;

}

bool RadosCatalogueReader::retrieve(const Key& key, Field& field) const {

    eckit::Log::debug<LibFdb5>() << "Trying to retrieve key " << key << std::endl;
    eckit::Log::debug<LibFdb5>() << "Scanning index " << current_.location() << std::endl;

    if (!current_.mayContain(key)) return false;

    return current_.get(key, fdb5::Key(), field);

}

static fdb5::CatalogueBuilder<fdb5::RadosCatalogueReader> builder("rados.reader");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
