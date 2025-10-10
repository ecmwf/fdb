/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/daos/DaosEngine.h"

#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"

#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void DaosEngine::configureDaos(const Config& config) const {

    if (daos_config_.has_value())
        return;

    daos_config_.emplace(eckit::LocalConfiguration());
    if (config.has("daos"))
        daos_config_.emplace(config.getSubConfiguration("daos"));
    if (daos_config_->has("client"))
        fdb5::DaosManager::instance().configure(daos_config_->getSubConfiguration("client"));
}

std::string DaosEngine::name() const {
    return DaosEngine::typeName();
}

bool DaosEngine::canHandle(const eckit::URI& uri, const Config& config) const {

    configureDaos(config);

    if (uri.scheme() != "daos")
        return false;

    fdb5::DaosName n{uri};

    if (!n.hasOID())
        return false;

    /// @todo: check containerName is not root_cont_. root_cont_ should be populated in
    ///   configureDaos as done in DaosCommon
    // bool is_root_name = (n.containerName().find(root_cont_) != std::string::npos);
    bool is_root_name  = false;
    bool is_store_name = (n.containerName().find("_") != std::string::npos);

    /// @note: performed RPCs:
    /// - generate oids (daos_obj_generate_oid)
    /// - db kv open (daos_kv_open)

    fdb5::DaosName n2{n.poolName(), n.containerName(), catalogue_kv_};
    bool is_catalogue_kv = (!is_root_name && !is_store_name && (n.OID() == n2.OID()));

    return is_catalogue_kv && n.exists();
}

std::vector<eckit::URI> DaosEngine::visitableLocations(const Key& key, const Config& config) const {

    /// @note: code mostly copied from DaosCommon
    /// @note: should rather use DaosCommon, but can't inherit from it here as DaosEngine is
    ///   always instantiated even if daos is not used, and then DaosCommon would be unnecessarily
    ///   initialised. If owning a private instance of DaosCommon here, then the private members of
    ///   DaosCommon are not accessible from here

    configureDaos(config);

    std::string pool      = "default";
    std::string root_cont = "root";

    if (daos_config_->has("catalogue"))
        pool = daos_config_->getSubConfiguration("catalogue").getString("pool", pool);
    if (daos_config_->has("catalogue"))
        root_cont = daos_config_->getSubConfiguration("catalogue").getString("root_cont", root_cont);

    pool      = eckit::Resource<std::string>("fdbDaosCataloguePool;$FDB_DAOS_CATALOGUE_POOL", pool);
    root_cont = eckit::Resource<std::string>("fdbDaosCatalogueRootCont;$FDB_DAOS_CATALOGUE_ROOT_CONT", root_cont);

    fdb5::DaosOID main_kv_oid{0, 0, DAOS_OT_KV_HASHED, OC_S1};  /// @todo: take oclass from config

    /// ---

    fdb5::DaosKeyValueName main_kv_name{pool, root_cont, main_kv_oid};

    fdb5::DaosSession s{};

    std::vector<eckit::URI> res{};

    /// @note: performed RPCs:
    /// - main kv open (daos_kv_open)

    /// @todo: use this Optional technique in all cases where an action needs to be performed
    ///   (i.e. not just throw an exception) if an object does not exist
    std::optional<fdb5::DaosKeyValue> main_kv;
    try {
        main_kv.emplace(s, main_kv_name);
    }
    catch (fdb5::DaosEntityNotFoundException& e) {
        return res;
    }

    /// @note: performed RPCs:
    /// - main kv list keys (daos_kv_list)
    for (const auto& k : main_kv->keys()) {

        try {

            /// @note: performed RPCs:
            /// - main kv get db location size (daos_kv_get without a buffer)
            /// - main kv get db location (daos_kv_get)
            uint64_t size = main_kv->size(k);
            std::vector<char> v(size);
            main_kv->get(k, v.data(), size);

            eckit::URI uri(std::string(v.begin(), v.end()));
            ASSERT(uri.scheme() == typeName());

            /// @todo: this exact deserialisation is performed twice. Once here and once
            ///   in DaosCatalogue::(uri, ...). Try to avoid one.
            fdb5::DaosKeyValueName db_kv_name{uri};

            /// @note: performed RPCs:
            /// - db kv open (daos_kv_open)
            /// - db key get size (daos_kv_get without a buffer)
            /// - db key get (daos_kv_get)
            fdb5::DaosKeyValue db_kv{s, db_kv_name};  /// @note: includes exist check
            std::vector<char> data;
            eckit::MemoryStream ms = db_kv.getMemoryStream(data, "key", "DB kv");
            fdb5::Key db_key(ms);

            if (db_key.match(key)) {

                LOG_DEBUG_LIB(LibFdb5) << " found match with " << main_kv_name.URI() << " at key " << k << std::endl;
                res.push_back(uri);
            }
        }
        catch (eckit::Exception& e) {
            eckit::Log::error() << "Error loading FDB database " << k << " from " << main_kv_name.URI() << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }
    }

    return res;
}

std::vector<URI> DaosEngine::visitableLocations(const metkit::mars::MarsRequest& request, const Config& config) const {

    /// @todo: in the POSIX backend, a set of possible visitable locations is first constructed
    ///   from the schema, and the set of visitable locations found in the root directories are
    ///   checked against that set. In the DAOS backend the first step is skipped.

    /// @note: code mostly copied from DaosCommon
    /// @note: should rather use DaosCommon, but can't inherit from it here as DaosEngine is
    ///   always instantiated even if daos is not used, and then DaosCommon would be unnecessarily
    ///   initialised. If owning a private instance of DaosCommon here, then the private members of
    ///   DaosCommon are not accessible from here

    configureDaos(config);

    std::string pool      = "default";
    std::string root_cont = "root";

    if (daos_config_->has("catalogue"))
        pool = daos_config_->getSubConfiguration("catalogue").getString("pool", pool);
    if (daos_config_->has("catalogue"))
        root_cont = daos_config_->getSubConfiguration("catalogue").getString("root_cont", root_cont);

    pool      = eckit::Resource<std::string>("fdbDaosCataloguePool;$FDB_DAOS_CATALOGUE_POOL", pool);
    root_cont = eckit::Resource<std::string>("fdbDaosCatalogueRootCont;$FDB_DAOS_CATALOGUE_ROOT_CONT", root_cont);

    fdb5::DaosOID main_kv_oid{0, 0, DAOS_OT_KV_HASHED, OC_S1};  /// @todo: take oclass from config

    /// ---

    fdb5::DaosKeyValueName main_kv_name{pool, root_cont, main_kv_oid};

    fdb5::DaosSession s{};

    std::vector<eckit::URI> res{};

    /// @note: performed RPCs:
    /// - main kv open (daos_kv_open)

    /// @todo: use this Optional technique in all cases where an action needs to be performed
    ///   (i.e. not just throw an exception) if an object does not exist
    std::optional<fdb5::DaosKeyValue> main_kv;
    try {
        main_kv.emplace(s, main_kv_name);
    }
    catch (fdb5::DaosEntityNotFoundException& e) {
        return res;
    }

    /// @note: performed RPCs:
    /// - main kv list keys (daos_kv_list)
    for (const auto& k : main_kv->keys()) {

        try {

            /// @note: performed RPCs:
            /// - main kv get db location size (daos_kv_get without a buffer)
            /// - main kv get db location (daos_kv_get)
            uint64_t size = main_kv->size(k);
            std::vector<char> v(size);
            main_kv->get(k, v.data(), size);

            eckit::URI uri(std::string(v.begin(), v.end()));
            ASSERT(uri.scheme() == typeName());

            /// @todo: this exact deserialisation is performed twice. Once here and once
            ///   in DaosCatalogue::(uri, ...). Try to avoid one.
            fdb5::DaosKeyValueName db_kv_name{uri};

            /// @note: performed RPCs:
            /// - db kv open (daos_kv_open)
            /// - db key get size (daos_kv_get without a buffer)
            /// - db key get (daos_kv_get)
            fdb5::DaosKeyValue db_kv{s, db_kv_name};
            std::vector<char> data;
            eckit::MemoryStream ms = db_kv.getMemoryStream(data, "key", "DB kv");
            fdb5::Key db_key(ms);

            if (db_key.partialMatch(request)) {

                LOG_DEBUG_LIB(LibFdb5) << " found match with " << main_kv_name.URI() << " at key " << k << std::endl;
                res.push_back(uri);
            }
        }
        catch (eckit::Exception& e) {
            eckit::Log::error() << "Error loading FDB database from " << main_kv_name.URI() << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }
    }

    return res;
}

static EngineBuilder<DaosEngine> daos_builder;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
