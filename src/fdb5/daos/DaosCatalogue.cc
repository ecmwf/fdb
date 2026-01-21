/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <optional>

#include "eckit/config/Resource.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/database/DatabaseNotFoundException.h"
#include "fdb5/database/WipeState.h"

#include "fdb5/daos/DaosCatalogue.h"
#include "fdb5/daos/DaosIndex.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

// using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosCatalogue::DaosCatalogue(const Key& key, const fdb5::Config& config) :
    CatalogueImpl(key, ControlIdentifiers{}, config), DaosCommon(config, "catalogue", key) {

    // TODO: apply the mechanism in RootManager::directory, using
    //   FileSpaceTables to determine root_pool_name_ according to key
    //   and using DbPathNamerTables to determine db_cont_name_ according
    //   to key
}

DaosCatalogue::DaosCatalogue(const eckit::URI& uri, const ControlIdentifiers& controlIdentifiers,
                             const fdb5::Config& config) :
    CatalogueImpl(Key(), controlIdentifiers, config), DaosCommon(config, "catalogue", uri) {

    // Read the real DB key into the DB base object
    try {

        fdb5::DaosSession s{};
        fdb5::DaosKeyValueName n{pool_, db_cont_, catalogue_kv_};
        fdb5::DaosKeyValue db_kv{s, n};

        std::vector<char> data;
        eckit::MemoryStream ms = db_kv.getMemoryStream(data, "key", "DB kv");
        dbKey_                 = fdb5::Key(ms);
    }
    catch (fdb5::DaosEntityNotFoundException& e) {

        throw fdb5::DatabaseNotFoundException(std::string("DaosCatalogue database not found ") + "(pool: '" + pool_ +
                                              "', container: '" + db_cont_ + "')");
    }
}

bool DaosCatalogue::exists() const {

    fdb5::DaosKeyValueName catalogue_kv_name{pool_, db_cont_, catalogue_kv_};
    return catalogue_kv_name.exists();
}

eckit::URI DaosCatalogue::uri() const {

    return fdb5::DaosName{db_kv_->poolName(), db_kv_->containerName()}.URI();
}

const Schema& DaosCatalogue::schema() const {
    return schema_;
}

const Rule& DaosCatalogue::rule() const {
    ASSERT(rule_);
    return *rule_;
}

void DaosCatalogue::loadSchema() {

    eckit::Timer timer("DaosCatalogue::loadSchema()", eckit::Log::debug<fdb5::LibFdb5>());

    /// @note: performed RPCs:
    /// - daos_obj_generate_oid
    /// - daos_kv_open
    /// - daos_kv_get without a buffer
    /// - daos_kv_get
    fdb5::DaosKeyValueName nkv{pool_, db_cont_, catalogue_kv_};
    fdb5::DaosSession s{};
    fdb5::DaosKeyValue kv{s, nkv};
    uint64_t size = kv.size("schema");
    std::vector<char> v(size);
    kv.get("schema", v.data(), size);

    std::istringstream stream{std::string(v.begin(), v.end())};
    schema_.load(stream);

    rule_ = &schema_.matchingRule(dbKey_);
}

bool DaosCatalogue::uriBelongs(const eckit::URI& uri) const {

    fdb5::DaosName n{uri};

    bool result = (uri.scheme() == type());
    result      = result && (n.poolName() == pool_);
    result      = result && (n.containerName().rfind(db_cont_, 0) == 0);
    result      = result && (n.OID().otype() == DAOS_OT_KV_HASHED);

    return result;
};

std::vector<Index> DaosCatalogue::indexes(bool) const {

    /// @note: sorted is not implemented as is not necessary in this backend.

    fdb5::DaosKeyValueName catalogue_kv_name{pool_, db_cont_, catalogue_kv_};
    fdb5::DaosSession s{};

    /// @note: performed RPCs:
    /// - db kv open (daos_kv_open)
    /// - db kv list keys (daos_kv_list)
    fdb5::DaosKeyValue catalogue_kv{s, catalogue_kv_name};  /// @note: throws if not exists

    std::vector<fdb5::Index> res;

    for (const auto& key : catalogue_kv.keys()) {

        /// @todo: document these well. Single source these reserved values.
        ///    Ensure where appropriate that user-provided keys do not collide.
        if (key == "schema" || key == "key")
            continue;

        /// @note: performed RPCs:
        /// - db kv get index location size (daos_kv_get without a buffer)
        /// - db kv get index location (daos_kv_get)
        uint64_t size{catalogue_kv.size(key)};
        std::vector<char> v(size);
        catalogue_kv.get(key, v.data(), size);

        fdb5::DaosKeyValueName index_kv_name{eckit::URI(std::string(v.begin(), v.end()))};

        /// @note: performed RPCs:
        /// - index kv open (daos_kv_open)
        /// - index kv get size (daos_kv_get without a buffer)
        /// - index kv get key (daos_kv_get)
        /// @note: the following three lines intend to check whether the index kv exists
        ///   or not. The DaosKeyValue constructor calls kv open, which always succeeds,
        ///   so it is not useful on its own to check whether the index KV existed or not.
        ///   Instead, presence of a "key" key in the KV is used to determine if the index
        ///   KV existed.
        fdb5::DaosKeyValue index_kv{s, index_kv_name};
        std::optional<fdb5::Key> index_key;
        try {
            std::vector<char> data;
            eckit::MemoryStream ms = index_kv.getMemoryStream(data, "key", "index KV");
            index_key.emplace(ms);
        }
        catch (fdb5::DaosEntityNotFoundException& e) {
            continue;  /// @note: the index_kv may not exist after a failed wipe
            /// @todo: the index_kv may exist even if it does not have the "key" key
        }

        res.push_back(Index(new fdb5::DaosIndex(index_key.value(), *this, index_kv_name, false)));
    }

    return res;
}

std::string DaosCatalogue::type() const {

    return fdb5::DaosEngine::typeName();
}

void DaosCatalogue::remove(const fdb5::DaosNameBase& n, std::ostream& logAlways, std::ostream& logVerbose, bool doit) {

    ASSERT(n.hasContainerName());

    logVerbose << "Removing " << (n.hasOID() ? "KV" : "container") << ": ";
    logAlways << n.URI() << std::endl;
    if (doit)
        n.destroy();
}

CatalogueWipeState DaosCatalogue::wipeInit() const {
    return CatalogueWipeState(dbKey_);
}

bool DaosCatalogue::wipeIndex(const Index& index, bool include, CatalogueWipeState& wipeState) const {

    fdb5::DaosKeyValueName location{index.location().uri()};
    const fdb5::DaosKeyValueName& db_kv = dbKeyValue();

    // If we have cross fdb-mounted another DB, ensure we can't delete another DBs data.
    if (!(location.containerName() == db_kv.containerName() && location.poolName() == db_kv.poolName())) {
        include = false;
    }

    // Add the axis and index paths to be removed.

    std::set<fdb5::DaosKeyValueName> axes;
    fdb5::DaosSession s{};
    fdb5::DaosKeyValue index_kv{s, location};  /// @todo: asserts that kv exists
    uint64_t size = index_kv.size("axes");
    if (size > 0) {
        std::vector<char> axes_data(size);
        index_kv.get("axes", &axes_data[0], size);
        std::vector<std::string> axis_names;
        eckit::Tokenizer parse(",");
        parse(std::string(axes_data.begin(), axes_data.end()), axis_names);
        std::string idx{index.key().valuesToString()};
        for (const auto& name : axis_names) {
            /// @todo: take oclass from config
            fdb5::DaosKeyValueOID oid(idx + std::string{"."} + name, OC_S1);
            fdb5::DaosKeyValueName nkv(location.poolName(), location.containerName(), oid);
            axes.insert(nkv);
        }
    }

    if (include) {
        /// @note: this actually means: mark for deindexing from databse KV
        wipeState.markForMasking(index);
        wipeState.markForDeletion(WipeElementType::CATALOGUE_INDEX, index.location().uri());
        for (const auto& axis : axes) {
            wipeState.markForDeletion(WipeElementType::CATALOGUE_INDEX, axis.URI());
        }
    }
    else {
        wipeState.markAsSafe({index.location().uri()});
        for (const auto& axis : axes) {
            wipeState.markAsSafe({axis.URI()});
        }
    }

    return include;
}

void DaosCatalogue::wipeFinalise(CatalogueWipeState& wipeState) const {

    // build database KV URI
    const fdb5::DaosKeyValueName& db_kv = dbKeyValue();
    fdb5::DaosName cont{db_kv.poolName(), db_kv.containerName()};
    eckit::URI db_kv_uri = db_kv.URI();

    /// @todo:
    // wipeState.setInfo("FDB Owner: " + owner());

    // ---- MARK FOR DELETION OR AS SAFE ----

    // We wipe everything if there is nothing within safeURIs - i.e. there is
    // no data that wasn't matched by the request
    bool wipeall = wipeState.safeURIs().empty();
    if (wipeall) {
        wipeState.markForDeletion(WipeElementType::CATALOGUE, {db_kv_uri});
    }
    else {
        wipeState.markAsSafe({db_kv_uri});
    }

    if (!wipeall) {
        return;
    }

    // Find any objects unaccounted for.
    std::vector<fdb5::DaosOID> allOIDs = cont.listOIDs();

    for (const fdb5::DaosOID& oid : allOIDs) {
        auto uri = fdb5::DaosName{cont.poolName(), cont.containerName(), oid}.URI();
        if (!(wipeState.isMarkedForDeletion(uri))) {
            wipeState.insertUnrecognised(uri);
        }
    }

    return;
}

void DaosCatalogue::maskIndexEntries(const std::set<Index>& indexes) const {

    for (const auto& index : indexes) {
        fdb5::DaosSession s{};
        fdb5::DaosKeyValue db_kv{s, dbKeyValue()};
        std::string key = index.key().valuesToString();
        /// @todo: should this fail if not present?
        if (db_kv.has(key)) {
            db_kv.remove(key);
        }
    }
}

bool DaosCatalogue::doWipeUnknown(const std::set<eckit::URI>& unknownURIs) const {
    for (const auto& uri : unknownURIs) {
        fdb5::DaosName name{uri};
        ASSERT(name.hasOID());
        if (name.exists()) {
            /// @todo: check in latest develop if still printed to stdout
            remove(name, std::cout, std::cout, true);
        }
    }

    return true;
}

bool DaosCatalogue::doWipe(const CatalogueWipeState& wipeState) const {
    bool wipeAll = wipeState.safeURIs().empty();  // nothing else in the container

    /// @note: this will remove the index and axis KVs, plus the database KV if wipeAll
    for (const auto& [type, uris] : wipeState.deleteMap()) {
        for (auto& uri : uris) {
            fdb5::DaosName name{uri};
            ASSERT(name.hasOID());
            remove(name, std::cout, std::cout, true);
        }
    }

    if (wipeAll) {
        fdb5::DaosName cont{pool_, db_cont_};
        emptyDatabases_.insert(cont.URI());
    }

    return true;
}

void DaosCatalogue::doWipeEmptyDatabases() const {

    if (emptyDatabases_.size() == 0)
        return;

    ASSERT(emptyDatabases_.size() == 1);

    // remove the database container
    fdb5::DaosName contName{*(emptyDatabases_.begin())};
    ASSERT(!contName.hasOID());
    if (contName.exists()) {
        ASSERT(contName.listOIDs().size() == 0);
        remove(contName, std::cout, std::cout, true);
    }
    // deindex database container from root KV
    fdb5::DaosSession s{};
    fdb5::DaosKeyValue rootKv{s, rootKeyValue()};
    std::string key = contName.containerName();
    /// @todo: should this fail if not present?
    if (rootKv.has(key)) {
        rootKv.remove(key);
    }

    emptyDatabases_.clear();
}

bool DaosCatalogue::doUnsafeFullWipe() const {

    // remove the database container
    fdb5::DaosName contName{pool_, db_cont_};
    ASSERT(!contName.hasOID());
    if (contName.exists()) {
        remove(contName, std::cout, std::cout, true);
    }
    // deindex database container from root KV
    fdb5::DaosSession s{};
    fdb5::DaosKeyValue rootKv{s, rootKeyValue()};
    std::string key = contName.containerName();
    /// @todo: should this fail if not present?
    if (rootKv.has(key)) {
        rootKv.remove(key);
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
