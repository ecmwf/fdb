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

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    if (currentIndexKey_ == key) {
        return true;
    }

    /// @todo: shouldn't this be set only if found a matching index?
    currentIndexKey_ = key;

    if (indexes_.find(key) == indexes_.end()) {

        fdb5::DaosKeyValueName catalogue_kv{pool_, db_cont_, catalogue_kv_};

        fdb5::DaosSession s{};

        /// @note: performed RPCs:
        /// - generate catalogue kv oid (daos_obj_generate_oid)
        /// - ensure catalogue kv exists (daos_kv_open)
        fdb5::StatsTimer st{"retrieve 01 catalogue kv open", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
        fdb5::DaosKeyValue catalogue_kv_obj{s, catalogue_kv};
        st.stop();

        int idx_loc_max_len = 512;  /// @todo: take from config
        std::vector<char> n((long) idx_loc_max_len);
        long res;

        try {

            /// @note: performed RPCs:
            /// - retrieve index kv location from catalogue kv (daos_kv_get)
            st.start("retrieve 02 catalogue kv get index", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            res = catalogue_kv_obj.get(key.valuesToString(), &n[0], idx_loc_max_len);
            st.stop();

        } catch (fdb5::DaosEntityNotFoundException& e) {

            st.stop();

            /// @note: performed RPCs:
            /// - close catalogue kv (daos_obj_close)
            st.start("retrieve 04 catalogue kv close", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));

            return false;

        }

        fdb5::DaosKeyValueName index_kv{eckit::URI{std::string{n.begin(), std::next(n.begin(), len - 1)}}};

        indexes_[key] = Index(new fdb5::DaosIndex(key, index_kv));

        indexes_[key].updatedAxes();

        /// @note: performed RPCs:
        /// - close catalogue kv (daos_obj_close)
        st.start("retrieve 04 catalogue kv close", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));

    }

    current_ = indexes_[key];

    return true;

}

void DaosCatalogueReader::deselectIndex() {

    NOTIMP; //< should not be called
    
}

bool DaosCatalogueReader::open() {

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    /// @note: performed RPCs:
    /// - daos_pool_connect
    /// - daos_cont_open
    /// - daos_obj_generate_oid
    /// - daos_kv_open
    fdb5::StatsTimer st{"retrieve 001 catalogue kv check exists", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    if (!DaosCatalogue::exists()) {
        return false;
    }
    st.stop();

    DaosCatalogue::loadSchema();
    return true;

}

void DaosCatalogueReader::axis(const std::string &keyword, eckit::StringSet &s) const {

    const eckit::DenseSet<std::string>& a = current_.axes().values(keyword);
    s.insert(a.begin(), a.end());

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
