/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/Buffer.h"
#include "eckit/config/Resource.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/LibFdb5.h"

#include "fdb5/daos/DaosCatalogue.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosIndex.h"
#include "fdb5/daos/DaosWipeVisitor.h"

// using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosCatalogue::DaosCatalogue(const Key& key, const fdb5::Config& config) :
    Catalogue(key, ControlIdentifiers{}, config), DaosCommon(config, "catalogue", key) {

    // TODO: apply the mechanism in RootManager::directory, using
    //   FileSpaceTables to determine root_pool_name_ according to key
    //   and using DbPathNamerTables to determine db_cont_name_ according
    //   to key

}

DaosCatalogue::DaosCatalogue(const eckit::URI& uri, const ControlIdentifiers& controlIdentifiers, const fdb5::Config& config) :
    Catalogue(Key(), controlIdentifiers, config), DaosCommon(config, "catalogue", uri) {

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    fdb5::StatsTimer st{"list/wipe 000 catalogue kv open and get db key", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

    // Read the real DB key into the DB base object
    fdb5::DaosSession s{};
    fdb5::DaosKeyValueName n{pool_, db_cont_, catalogue_kv_};
    fdb5::DaosKeyValue db_kv{s, n};

    int db_key_max_len = 512;  /// @todo: take from config
    std::vector<char> dbkey_data((long) db_key_max_len);
    long res = db_kv.get("key", &dbkey_data[0], db_key_max_len);

    st.stop();

    eckit::MemoryStream ms{&dbkey_data[0], res};
    dbKey_ = fdb5::Key(ms);

}

bool DaosCatalogue::exists() const {

    fdb5::DaosKeyValueName catalogue_kv_name{pool_, db_cont_, catalogue_kv_};
    return catalogue_kv_name.exists();

}

eckit::URI DaosCatalogue::uri() const {

    return fdb5::DaosName{db_kv_->poolName(), db_kv_->contName()}.URI();

}

const Schema& DaosCatalogue::schema() const {

    return schema_;

}

void DaosCatalogue::visitEntries(EntryVisitor& visitor, const Store& store, bool sorted) {

    std::vector<Index> all = indexes(sorted);
    // Allow the visitor to selectively reject this DB.
    if (visitor.visitDatabase(*this, store)) {
        if (visitor.visitIndexes()) {
            for (const Index& idx : all) {
                if (visitor.visitEntries()) {
                    idx.entries(visitor); // contains visitIndex
                } else {
                    visitor.visitIndex(idx);
                }
            }
        }

        visitor.catalogueComplete(*this);
    }

}

void DaosCatalogue::loadSchema() {

    using namespace std::placeholders;
    eckit::Timer& t = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    eckit::Timer timer("DaosCatalogue::loadSchema()", eckit::Log::debug<fdb5::LibFdb5>());

    /// @note: performed RPCs:
    /// - daos_obj_generate_oid
    /// - daos_kv_open
    /// - daos_kv_get without a buffer
    /// - daos_kv_get
    fdb5::StatsTimer st{"retrieve 002 catalogue kv get schema", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    fdb5::DaosKeyValueName nkv{pool_, db_cont_, catalogue_kv_};
    fdb5::DaosSession s{};
    fdb5::DaosKeyValue kv{s, nkv};
    daos_size_t size = kv.size("schema");
    std::vector<char> v(size);
    kv.get("schema", v.data(), size);
    t.stop();

    std::istringstream stream{std::string(v.begin(), v.end())};
    schema_.load(stream);

}

WipeVisitor* DaosCatalogue::wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const {
    return new DaosWipeVisitor(*this, store, request, out, doit, porcelain, unsafeWipeAll);
}

std::vector<Index> DaosCatalogue::indexes(bool sorted) const {

    /// @todo: implement sorted

    fdb5::DaosKeyValueName catalogue_kv_name{pool_, db_cont_, catalogue_kv_};
    ASSERT(catalogue_kv_name.exists());
    fdb5::DaosSession s{};
    fdb5::DaosKeyValue catalogue_kv{s, catalogue_kv_name};

    std::vector<fdb5::Index> res;

    for (const auto& key : catalogue_kv.keys()) {

        if (key == "schema" || key == "key") continue;

        daos_size_t size{catalogue_kv.size(key)};
        std::vector<char> v(size);
        catalogue_kv.get(key, v.data(), size);
        fdb5::DaosKeyValueName index_kv_name{eckit::URI(std::string(v.begin(), v.end()))};

        /// @todo: what if the index KV does not exist? It's plausible as a wipe may fail right after 
        ///   having deleted an index KV but before having deleted/punched the catalogue KV reference.
        ///   If we just ignore and continue, the vector returned by this function will not be consistent
        ///   with the actual content of the catalogue KV. It will not return an Index for that dangling
        ///   reference. And DaosCatalogue::wipe will never see nor be able to delete that reference.
        if (!index_kv_name.exists()) continue;

        fdb5::DaosKeyValue index_kv{s, index_kv_name};
        size = index_kv.size("key");
        std::vector<char> indexkey_data((long) size);
        index_kv.get("key", &indexkey_data[0], size);
        eckit::MemoryStream ms{&indexkey_data[0], size};
        fdb5::Key index_key(ms);

        res.push_back(Index(new fdb5::DaosIndex(index_key, index_kv_name)));

    }

    return res;
    
}

std::string DaosCatalogue::type() const {

    return DaosCatalogue::catalogueTypeName();
    
}

void DaosCatalogue::remove(const fdb5::DaosNameBase& n, std::ostream& logAlways, std::ostream& logVerbose, bool doit) {

    ASSERT(n.hasContName());

    logVerbose << "Removing " << (n.hasOID() ? "KV" : "container") << ": ";
    logAlways << n.URI() << std::endl;
    if (doit) n.destroy();

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
