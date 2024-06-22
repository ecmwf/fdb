/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "eckit/exception/Exceptions.h"
#include "eckit/config/Resource.h"
#include "eckit/utils/Tokenizer.h"

#include "fdb5/rados/RadosCommon.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosCommon::RadosCommon(const fdb5::Config& config, const std::string& component, const fdb5::Key& key) {

    std::vector<std::string> valid{"catalogue", "store"};
    ASSERT(std::find(valid.begin(), valid.end(), component) != valid.end());

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    db_namespace_ = key.valuesToString();

    readConfig(config, component, true);

  #ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE
    root_kv_.emplace(pool_, root_namespace_, "main_kv", true);
    db_kv_.emplace(pool_, db_namespace_, "catalogue_kv", true);
  #else
    root_kv_.emplace(pool_, root_namespace_, "main_kv");
    db_kv_.emplace(pool_, db_namespace_, "catalogue_kv");
  #endif

#else

    readConfig(config, component, true);

    db_pool_ = prefix_ + "_" + key.valuesToString();

  #ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE
    root_kv_.emplace(root_pool_, namespace_, "main_kv", true);
    db_kv_.emplace(db_pool_, namespace_, "catalogue_kv", true);
  #else
    root_kv_.emplace(root_pool_, namespace_, "main_kv");
    db_kv_.emplace(db_pool_, namespace_, "catalogue_kv");
  #endif

#endif

}

RadosCommon::RadosCommon(const fdb5::Config& config, const std::string& component, const eckit::URI& uri) {

    /// @note: validity of input URI is not checked here because this constructor is only triggered
    ///   by DB::buildReader in EntryVisitMechanism, where validity of URIs is ensured beforehand

    eckit::RadosKeyValue db_name{uri};

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    pool_ = db_name.nspace().pool().name();
    db_namespace_ = db_name.nspace().name();

    readConfig(config, component, false);

  #ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE
    root_kv_.emplace(pool_, root_namespace_, "main_kv", true);
    db_kv_.emplace(pool_, db_namespace_, "catalogue_kv", true);
  #else
    root_kv_.emplace(pool_, root_namespace_, "main_kv");
    db_kv_.emplace(pool_, db_namespace_, "catalogue_kv");
  #endif

#else

    db_pool_ = db_name.nspace().pool().name();
    namespace_ = db_name.nspace().name();

    readConfig(config, component, false);

    const auto parts = eckit::Tokenizer("_").tokenize(db_pool_);
    const auto n = parts.size();
    ASSERT(n > 1);
    prefix_ = parts[0];

  #ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE
    root_kv_.emplace(root_pool_, namespace_, "main_kv", true);
    db_kv_.emplace(db_pool_, namespace_, "catalogue_kv", true);
  #else
    root_kv_.emplace(root_pool_, namespace_, "main_kv");
    db_kv_.emplace(db_pool_, namespace_, "catalogue_kv");
  #endif

#endif

}

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
void RadosCommon::readConfig(const fdb5::Config& config, const std::string& component, bool readPool) {
#else
void RadosCommon::readConfig(const fdb5::Config& config, const std::string& component, bool readNamespace) {
#endif

    eckit::LocalConfiguration c{};

    if (config.has("rados")) c = config.getSubConfiguration("rados");

    maxObjectSize_ = c.getInt("maxObjectSize", 0);

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
    if (c.has(component)) root_namespace_ = c.getSubConfiguration(component).getString("root_namespace", root_namespace_);

    if (readPool)
        pool_ = eckit::Resource<std::string>("fdbRados" + first_cap + "Pool;$FDB_RADOS_" + all_caps + "_POOL", pool_);
    root_namespace_ = eckit::Resource<std::string>("fdbRados" + first_cap + "RootNamespace;$FDB_RADOS_" + all_caps + "_ROOT_NAMESPACE", root_namespace_);

#else

    if (readNamespace) namespace_ = "default";
    root_pool_ = "root";

    if (readNamespace)
        if (c.has(component)) namespace_ = c.getSubConfiguration(component).getString("namespace", namespace_);
    if (c.has(component)) root_pool_ = c.getSubConfiguration(component).getString("root_pool", root_pool_);

    if (readNamespace)
        namespace_ = eckit::Resource<std::string>("fdbRados" + first_cap + "Namespace;$FDB_RADOS_" + all_caps + "_NAMESPACE", namespace_);
    root_pool_ = eckit::Resource<std::string>("fdbRados" + first_cap + "RootPool;$FDB_RADOS_" + all_caps + "_ROOT_POOL", root_pool_);

    prefix_ = c.getString("pool_prefix", prefix_);
    if (c.has(component)) prefix_ = c.getSubconfiguration(component).getString("pool_prefix", prefix_);
    ASSERT_MSG(prefix_.find("_") == std::string::npos, "The configured pool prefix must not contain underscores.");

#endif

    // if (c.has("client"))
    //     fdb5::DaosManager::instance().configure(c.getSubConfiguration("client"));

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5