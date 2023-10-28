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
#include "fdb5/daos/DaosEngine.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosName.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void DaosEngine::configureDaos(const Config& config) const {

    if (daos_config_.has_value()) return;

    daos_config_.emplace(eckit::LocalConfiguration());
    if (config.has("daos")) daos_config_.emplace(config.getSubConfiguration("daos"));
    if (daos_config_->has("client"))
        fdb5::DaosManager::instance().configure(daos_config_->getSubConfiguration("client"));

}

std::string DaosEngine::name() const {
    return DaosEngine::typeName();
}

bool DaosEngine::canHandle(const eckit::URI& uri, const Config& config) const {

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();
    
    configureDaos(config);

    if (uri.scheme() != "daos")
        return false;

    fdb5::DaosName n{uri};

    if (!n.hasOID()) return false;

    /// @todo: check contName is not root_cont_. root_cont_ should be populated in
    ///   configureDaos as done in DaosCommon
    // bool is_root_name = (n.contName().find(root_cont_) != std::string::npos);
    bool is_root_name = false;
    bool is_store_name = (n.contName().find("_") != std::string::npos);

    /// @note: performed RPCs:
    /// - generate oids (daos_obj_generate_oid)
    /// - db kv open (daos_kv_open)
    fdb5::StatsTimer st{"list/wipe 005 db kv checks", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    
    fdb5::DaosName n2{n.poolName(), n.contName(), catalogue_kv_};
    n2.generateOID();
    bool is_catalogue_kv = (!is_root_name && !is_store_name && (n.OID() == n2.OID()));

    return is_catalogue_kv && n.exists();

}

std::unique_ptr<DB> DaosEngine::buildReader(const eckit::URI& uri, const Config& config) const {

    Log::debug<LibFdb5>() << "FDB processing DAOS DB at KV " << uri << std::endl;

    return DB::buildReader(uri, config);

}

bool DaosEngine::toExistingDBURI(eckit::URI& uri, const Config& config) const {

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    configureDaos(config);

    fdb5::DaosName n{uri};
    if (!n.hasContName()) return false;

    fdb5::DaosName n2{n.poolName(), n.contName(), catalogue_kv_};

    /// @note: performed RPCs:
    /// - db kv open (daos_kv_open)
    fdb5::StatsTimer st{"list/wipe 004 db kv check exists", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    if (!n2.exists()) return false;
    st.stop();

    uri = n2.URI();
    return true;

}

std::vector<eckit::URI> DaosEngine::visitableLocations(const Key& key, const Config& config) const
{

    /// @note: code mostly copied from DaosCommon
    /// @note: should rather use DaosCommon, but can't inherit from it here as DaosEngine is 
    ///   always instantiated even if daos is not used, and then DaosCommon would be unnecessarily 
    ///   initialised. If owning a private instance of DaosCommon here, then the private members of 
    ///   DaosCommon are not accessible from here

    configureDaos(config);

    std::string pool = "default";
    std::string root_cont = "root";

    if (daos_config_->has("catalogue")) pool = daos_config_->getSubConfiguration("catalogue").getString("pool", pool);
    if (daos_config_->has("catalogue")) root_cont = daos_config_->getSubConfiguration("catalogue").getString("root_cont", root_cont);

    pool = eckit::Resource<std::string>("fdbDaosCataloguePool;$FDB_DAOS_CATALOGUE_POOL", pool);
    root_cont = eckit::Resource<std::string>("fdbDaosCatalogueRootCont;$FDB_DAOS_CATALOGUE_ROOT_CONT", root_cont);

    fdb5::DaosOID main_kv_oid{0, 0, DAOS_OT_KV_HASHED, OC_S1};  /// @todo: take oclass from config

    /// ---

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    fdb5::DaosKeyValueName main_kv_name{pool, root_cont, main_kv_oid};

    fdb5::DaosSession s{};

    std::vector<eckit::URI> res{};

    /// @note: performed RPCs:
    /// - main kv open (daos_kv_open)
    fdb5::StatsTimer st{"list/wipe 000 main kv open and get keys", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

    /// @todo: use this Optional technique in all cases where an action needs to be performed
    ///   (i.e. not just throw an exception) if an object does not exist
    eckit::Optional<fdb5::DaosKeyValue> main_kv;
    try {
        main_kv.emplace(s, main_kv_name);
    } catch (fdb5::DaosEntityNotFoundException& e) {
        return res;
    }

    st.stop();

    /// @note: performed RPCs:
    /// - main kv list keys (daos_kv_list)
    st.start("list/wipe 001 main kv list", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
    for (const auto& k : main_kv->keys()) {
        st.stop();

        try {

            /// @note: performed RPCs:
            /// - main kv get db location size (daos_kv_get without a buffer)
            /// - main kv get db location (daos_kv_get)
            st.start("list/wipe 002 main kv get db location", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            daos_size_t size = main_kv->size(k);
            std::vector<char> v(size);
            main_kv->get(k, v.data(), size);
            st.stop();

            eckit::URI uri(std::string(v.begin(), v.end()));
            ASSERT(uri.scheme() == typeName());

            /// @todo: this exact deserialisation is performed twice. Once here and once 
            ///   in DaosCatalogue::(uri, ...). Try to avoid one.
            fdb5::DaosKeyValueName db_kv_name{uri};

            /// @note: performed RPCs:
            /// - db kv open (daos_kv_open)
            /// - db key get size (daos_kv_get without a buffer)
            /// - db key get (daos_kv_get)
            st.start("list/wipe 003 db kv open and get key", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            fdb5::DaosKeyValue db_kv{s, db_kv_name};  /// @note: includes exist check
            size = db_kv.size("key");
            if (size == 0) throw eckit::Exception("Key 'key' not found in DB kv");
            std::vector<char> dbkey_data((long) size);
            db_kv.get("key", &dbkey_data[0], size);
            st.stop();

            eckit::MemoryStream ms{&dbkey_data[0], size};
            fdb5::Key db_key(ms);

            if (db_key.match(key)) {

                Log::debug<LibFdb5>() << " found match with " << main_kv_name.URI() << " at key " << k << std::endl;
                res.push_back(uri);

            }
        
        } catch (eckit::Exception& e) {
            eckit::Log::error() <<  "Error loading FDB database " << k << " from " << main_kv_name.URI() << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }

    }
    st.stop();

    return res;

}

std::vector<URI> DaosEngine::visitableLocations(const metkit::mars::MarsRequest& request, const Config& config) const
{

    /// @note: code mostly copied from DaosCommon
    /// @note: should rather use DaosCommon, but can't inherit from it here as DaosEngine is 
    ///   always instantiated even if daos is not used, and then DaosCommon would be unnecessarily 
    ///   initialised. If owning a private instance of DaosCommon here, then the private members of 
    ///   DaosCommon are not accessible from here

    configureDaos(config);

    std::string pool = "default";
    std::string root_cont = "root";

    if (daos_config_->has("catalogue")) pool = daos_config_->getSubConfiguration("catalogue").getString("pool", pool);
    if (daos_config_->has("catalogue")) root_cont = daos_config_->getSubConfiguration("catalogue").getString("root_cont", root_cont);

    pool = eckit::Resource<std::string>("fdbDaosCataloguePool;$FDB_DAOS_CATALOGUE_POOL", pool);
    root_cont = eckit::Resource<std::string>("fdbDaosCatalogueRootCont;$FDB_DAOS_CATALOGUE_ROOT_CONT", root_cont);

    fdb5::DaosOID main_kv_oid{0, 0, DAOS_OT_KV_HASHED, OC_S1};  /// @todo: take oclass from config

    /// ---

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    fdb5::DaosKeyValueName main_kv_name{pool, root_cont, main_kv_oid};

    fdb5::DaosSession s{};

    std::vector<eckit::URI> res{};

    /// @note: performed RPCs:
    /// - main kv open (daos_kv_open)
    fdb5::StatsTimer st{"list/wipe 000 main kv open and get keys", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

    /// @todo: use this Optional technique in all cases where an action needs to be performed
    ///   (i.e. not just throw an exception) if an object does not exist
    eckit::Optional<fdb5::DaosKeyValue> main_kv;
    try {
        main_kv.emplace(s, main_kv_name);
    } catch (fdb5::DaosEntityNotFoundException& e) {
        return res;
    }

    st.stop();

    /// @note: performed RPCs:
    /// - main kv list keys (daos_kv_list)
    st.start("list/wipe 001 main kv list", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
    for (const auto& k : main_kv->keys()) {
        st.stop();

        try {

            /// @note: performed RPCs:
            /// - main kv get db location size (daos_kv_get without a buffer)
            /// - main kv get db location (daos_kv_get)
            st.start("list/wipe 002 main kv get db location", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            daos_size_t size = main_kv->size(k);
            std::vector<char> v(size);
            main_kv->get(k, v.data(), size);
            st.stop();

            eckit::URI uri(std::string(v.begin(), v.end()));
            ASSERT(uri.scheme() == typeName());

            /// @todo: this exact deserialisation is performed twice. Once here and once 
            ///   in DaosCatalogue::(uri, ...). Try to avoid one.
            fdb5::DaosKeyValueName db_kv_name{uri};

            /// @note: performed RPCs:
            /// - db kv open (daos_kv_open)
            /// - db key get size (daos_kv_get without a buffer)
            /// - db key get (daos_kv_get)
            st.start("list/wipe 003 db kv open and get key", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
            fdb5::DaosKeyValue db_kv{s, db_kv_name};
            size = db_kv.size("key");
            if (size == 0) throw eckit::Exception("Key 'key' not found in DB kv");
            std::vector<char> dbkey_data((long) size);
            db_kv.get("key", &dbkey_data[0], size);
            st.stop();

            eckit::MemoryStream ms{&dbkey_data[0], size};
            fdb5::Key db_key(ms);

            if (db_key.partialMatch(request)) {

                Log::debug<LibFdb5>() << " found match with " << main_kv_name.URI() << " at key " << k << std::endl;
                res.push_back(uri);

            }
        
        } catch (eckit::Exception& e) {
            eckit::Log::error() <<  "Error loading FDB database from " << main_kv_name.URI() << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }

    }

    return res;

}

static EngineBuilder<DaosEngine> daos_builder;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
