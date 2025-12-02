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

#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

#include "fdb5/daos/DaosCatalogueReader.h"
#include "fdb5/daos/DaosIndex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// @note: as opposed to the TOC catalogue, the DAOS catalogue does not pre-load all indexes from storage.
///   Instead, it selects and loads only those indexes that are required to fulfil the request.

DaosCatalogueReader::DaosCatalogueReader(const Key& dbKey, const fdb5::Config& config) : DaosCatalogue(dbKey, config) {

    /// @todo: schema is being loaded at DaosCatalogueWriter creation for write, but being loaded
    ///        at DaosCatalogueReader::open for read. Is this OK?
}

DaosCatalogueReader::DaosCatalogueReader(const eckit::URI& uri, const fdb5::Config& config) :
    DaosCatalogue(uri, ControlIdentifiers{}, config) {}

bool DaosCatalogueReader::selectIndex(const Key& idxKey) {

    if (currentIndexKey_ == idxKey) {
        return true;
    }

    /// @todo: shouldn't this be set only if found a matching index?
    currentIndexKey_ = idxKey;

    if (indexes_.find(idxKey) == indexes_.end()) {

        fdb5::DaosKeyValueName catalogue_kv{pool_, db_cont_, catalogue_kv_};

        fdb5::DaosSession s{};

        /// @note: performed RPCs:
        /// - generate catalogue kv oid (daos_obj_generate_oid)
        /// - ensure catalogue kv exists (daos_kv_open)
        fdb5::DaosKeyValue catalogue_kv_obj{s, catalogue_kv};

        int idx_loc_max_len = 512;  /// @todo: take from config
        std::vector<char> n((long)idx_loc_max_len);
        long res;

        try {

            /// @note: performed RPCs:
            /// - retrieve index kv location from catalogue kv (daos_kv_get)
            res = catalogue_kv_obj.get(idxKey.valuesToString(), &n[0], idx_loc_max_len);
        }
        catch (fdb5::DaosEntityNotFoundException& e) {

            /// @note: performed RPCs:
            /// - close catalogue kv (daos_obj_close)

            return false;
        }

        fdb5::DaosKeyValueName index_kv{eckit::URI{std::string{n.begin(), std::next(n.begin(), res)}}};

        indexes_[idxKey] = Index(new fdb5::DaosIndex(idxKey, *this, index_kv, true));

        /// @note: performed RPCs:
        /// - close catalogue kv (daos_obj_close)
    }

    current_ = indexes_[idxKey];

    return true;
}

void DaosCatalogueReader::deselectIndex() {

    NOTIMP;  //< should not be called
}

bool DaosCatalogueReader::open() {

    /// @note: performed RPCs:
    /// - daos_pool_connect
    /// - daos_cont_open
    /// - daos_obj_generate_oid
    /// - daos_kv_open
    if (!DaosCatalogue::exists()) {
        return false;
    }

    DaosCatalogue::loadSchema();
    return true;
}

std::optional<Axis> DaosCatalogueReader::computeAxis(const std::string& keyword) const {

    Axis s;

    bool found = false;
    if (current_.axes().has(keyword)) {
        found = true;
        s.merge(current_.axes().values(keyword));
    }

    if (found) {
        return s;
    }
    return std::nullopt;
}

bool DaosCatalogueReader::retrieve(const Key& key, Field& field) const {

    eckit::Log::debug<LibFdb5>() << "Trying to retrieve key " << key << std::endl;
    eckit::Log::debug<LibFdb5>() << "Scanning index " << current_.location() << std::endl;

    if (!current_.mayContain(key)) {
        return false;
    }

    return current_.get(key, fdb5::Key(), field);
}

static fdb5::CatalogueReaderBuilder<fdb5::DaosCatalogueReader> builder("daos");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
