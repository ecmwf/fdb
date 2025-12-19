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

#include "fdb5/daos/DaosCatalogue.h"
#include "fdb5/daos/DaosIndex.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosWipeVisitor.h"

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
        dbKey_ = fdb5::Key(ms);
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

WipeVisitor* DaosCatalogue::wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out,
                                        bool doit, bool porcelain, bool unsafeWipeAll) const {
    return new DaosWipeVisitor(*this, store, request, out, doit, porcelain, unsafeWipeAll);
}

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
        if (key == "schema" || key == "key") {
            continue;
        }

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
    if (doit) {
        n.destroy();
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
