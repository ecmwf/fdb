/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "eckit/serialisation/MemoryStream.h"
#include "eckit/config/Resource.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/rados/RadosEngine.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::string RadosEngine::name() const {
    return RadosEngine::typeName();
}

// bool DaosEngine::canHandle(const eckit::URI& uri, const Config& config) const {

//     configureDaos(config);

//     if (uri.scheme() != "daos")
//         return false;

//     fdb5::DaosName n{uri};

//     if (!n.hasOID()) return false;

//     /// @todo: check containerName is not root_cont_. root_cont_ should be populated in
//     ///   configureDaos as done in DaosCommon
//     // bool is_root_name = (n.containerName().find(root_cont_) != std::string::npos);
//     bool is_root_name = false;
//     bool is_store_name = (n.containerName().find("_") != std::string::npos);

//     /// @note: performed RPCs:
//     /// - generate oids (daos_obj_generate_oid)
//     /// - db kv open (daos_kv_open)

//     fdb5::DaosName n2{n.poolName(), n.containerName(), catalogue_kv_};
//     bool is_catalogue_kv = (!is_root_name && !is_store_name && (n.OID() == n2.OID()));

//     return is_catalogue_kv && n.exists();

// }

std::vector<eckit::URI> RadosEngine::visitableLocations(const Key& key, const Config& config) const
{

    /// @note: code mostly copied from DaosCommon
    /// @note: should rather use DaosCommon, but can't inherit from it here as DaosEngine is 
    ///   always instantiated even if daos is not used, and then DaosCommon would be unnecessarily 
    ///   initialised. If owning a private instance of DaosCommon here, then the private members of 
    ///   DaosCommon are not accessible from here

    std::string component = "catalogue";

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    readConfig(config, component, true);

    // db_namespace_ = nspace_prefix_ + "_" + key.valuesToString();

    root_kv_.emplace(pool_, root_namespace_, "main_kv");
    // db_kv_.emplace(pool_, db_namespace_, "catalogue_kv");

#else

    readConfig(config, component, true);

    // db_pool_ = pool_prefix_ + "_" + key.valuesToString();

    root_kv_.emplace(root_pool_, namespace_, "main_kv");
    // db_kv_.emplace(db_pool_, namespace_, "catalogue_kv");

#endif

    /// ---

    std::vector<eckit::URI> res{};

    /// @note: performed RPCs:
    /// - main kv open (daos_kv_open)
 
    if (!root_kv_->exists()) return res;

    /// @note: performed RPCs:
    /// - main kv list keys (daos_kv_list)
    for (const auto& k : root_kv_->keys()) {

        try {

            /// @note: performed RPCs:
            /// - main kv get db location size (daos_kv_get without a buffer)
            /// - main kv get db location (daos_kv_get)
            std::vector<char> v;
            auto m = root_kv_->getMemoryStream(v, k, "root kv");

            eckit::URI uri(std::string(v.begin(), v.end()));
            ASSERT(uri.scheme() == typeName());

            /// @todo: this exact deserialisation is performed twice. Once here and once 
            ///   in DaosCatalogue::(uri, ...). Try to avoid one.

            /// @note: performed RPCs:
            /// - db kv open (daos_kv_open)
            /// - db key get size (daos_kv_get without a buffer)
            /// - db key get (daos_kv_get)
            eckit::RadosKeyValue db_kv{uri};  /// @note: includes exist check
            std::vector<char> data;
            eckit::MemoryStream ms = db_kv.getMemoryStream(data, "key", "DB kv");
            fdb5::Key db_key(ms);

            if (db_key.match(key)) {

                Log::debug<LibFdb5>() << " found match with " << root_kv_->uri() << " at key " << k << std::endl;
                res.push_back(uri);

            }
        
        } catch (eckit::Exception& e) {
            eckit::Log::error() <<  "Error loading FDB database " << k << " from " << root_kv_->uri() << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }

    }

    return res;

}

std::vector<URI> RadosEngine::visitableLocations(const metkit::mars::MarsRequest& request, const Config& config) const
{

    /// @note: code mostly copied from DaosCommon
    /// @note: should rather use DaosCommon, but can't inherit from it here as DaosEngine is 
    ///   always instantiated even if daos is not used, and then DaosCommon would be unnecessarily 
    ///   initialised. If owning a private instance of DaosCommon here, then the private members of 
    ///   DaosCommon are not accessible from here

    std::string component = "catalogue";

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    readConfig(config, component, true);

    // db_namespace_ = nspace_prefix_ + "_" + key.valuesToString();

    root_kv_.emplace(pool_, root_namespace_, "main_kv");
    // db_kv_.emplace(pool_, db_namespace_, "catalogue_kv");

#else

    readConfig(config, component, true);

    // db_pool_ = pool_prefix_ + "_" + key.valuesToString();

    root_kv_.emplace(root_pool_, namespace_, "main_kv");
    // db_kv_.emplace(db_pool_, namespace_, "catalogue_kv");

#endif

    /// ---

    std::vector<eckit::URI> res{};

    /// @note: performed RPCs:
    /// - main kv open (daos_kv_open)
 
    if (!root_kv_->exists()) return res;

    /// @note: performed RPCs:
    /// - main kv list keys (daos_kv_list)
    for (const auto& k : root_kv_->keys()) {

        try {

            /// @note: performed RPCs:
            /// - main kv get db location size (daos_kv_get without a buffer)
            /// - main kv get db location (daos_kv_get)
            std::vector<char> v;
            auto m = root_kv_->getMemoryStream(v, k, "root kv");

            eckit::URI uri(std::string(v.begin(), v.end()));
            ASSERT(uri.scheme() == typeName());

            /// @todo: this exact deserialisation is performed twice. Once here and once 
            ///   in DaosCatalogue::(uri, ...). Try to avoid one.

            /// @note: performed RPCs:
            /// - db kv open (daos_kv_open)
            /// - db key get size (daos_kv_get without a buffer)
            /// - db key get (daos_kv_get)
            eckit::RadosKeyValue db_kv{uri};  /// @note: includes exist check
            std::vector<char> data;
            eckit::MemoryStream ms = db_kv.getMemoryStream(data, "key", "DB kv");
            fdb5::Key db_key(ms);

            if (db_key.partialMatch(request)) {

                Log::debug<LibFdb5>() << " found match with " << root_kv_->uri() << " at key " << k << std::endl;
                res.push_back(uri);

            }
        
        } catch (eckit::Exception& e) {
            eckit::Log::error() <<  "Error loading FDB database " << k << " from " << root_kv_->uri() << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }

    }

    return res;

}

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
void RadosEngine::readConfig(const fdb5::Config& config, const std::string& component, bool readPool) const {
#else
void RadosEngine::readConfig(const fdb5::Config& config, const std::string& component, bool readNamespace) const {
#endif

    eckit::LocalConfiguration c{};

    if (config.has("rados")) c = config.getSubConfiguration("rados");

    // maxObjectSize_ = c.getInt("maxObjectSize", 0);

    std::string first_cap{component};
    first_cap[0] = toupper(component[0]);

    std::string all_caps{component};
    for (auto & c: all_caps) c = toupper(c);

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    if (readPool) pool_ = "default";
    root_namespace_ = "root";

    if (readPool) {
        pool_ = c.getString("pool", pool_);
        if (c.has(component)) pool_ = c.getSubConfiguration(component).getString("pool", pool_);
    }
    root_namespace_ = c.getString("root_namespace", root_namespace_);
    if (c.has(component)) root_namespace_ = c.getSubConfiguration(component).getString("root_namespace", root_namespace_);

    if (readPool)
        pool_ = eckit::Resource<std::string>("fdbRados" + first_cap + "Pool;$FDB_RADOS_" + all_caps + "_POOL", pool_);
    root_namespace_ = eckit::Resource<std::string>("fdbRados" + first_cap + "RootNamespace;$FDB_RADOS_" + all_caps + "_ROOT_NAMESPACE", root_namespace_);

    nspace_prefix_ = c.getString("namespace_prefix", nspace_prefix_);
    if (c.has(component)) nspace_prefix_ = c.getSubConfiguration(component).getString("namespace_prefix", nspace_prefix_);
    ASSERT_MSG(nspace_prefix_.find("_") == std::string::npos, "The configured namespace prefix must not contain underscores.");

#else

    if (readNamespace) namespace_ = "default";
    root_pool_ = "root";

    if (readNamespace)
        namespace_ = c.getString("namespace", namespace_);
        if (c.has(component)) namespace_ = c.getSubConfiguration(component).getString("namespace", namespace_);
    root_pool_ = c.getString("root_pool", root_pool_);
    if (c.has(component)) root_pool_ = c.getSubConfiguration(component).getString("root_pool", root_pool_);

    if (readNamespace)
        namespace_ = eckit::Resource<std::string>("fdbRados" + first_cap + "Namespace;$FDB_RADOS_" + all_caps + "_NAMESPACE", namespace_);
    root_pool_ = eckit::Resource<std::string>("fdbRados" + first_cap + "RootPool;$FDB_RADOS_" + all_caps + "_ROOT_POOL", root_pool_);

    pool_prefix_ = c.getString("pool_prefix", pool_prefix_);
    if (c.has(component)) pool_prefix_ = c.getSubConfiguration(component).getString("pool_prefix", pool_prefix_);
    ASSERT_MSG(pool_prefix_.find("_") == std::string::npos, "The configured pool prefix must not contain underscores.");

#endif

}

static EngineBuilder<RadosEngine> rados_builder;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
