/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/rados/RadosCommon.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/utils/Tokenizer.h"

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosCommon::RadosCommon(const Config& config, const std::string& component, const Key& key) {

    std::vector<std::string> valid{"catalogue", "store"};
    ASSERT(std::find(valid.begin(), valid.end(), component) != valid.end());

    readConfig(config, component, true);

    db_namespace_ = nspace_prefix_ + "_" + key.valuesToString();

    root_kv_.emplace(pool_, root_namespace_, "main_kv");
    db_kv_.emplace(pool_, db_namespace_, "catalogue_kv");
}

RadosCommon::RadosCommon(const Config& config, const std::string& component, const eckit::URI& uri) {

    // Accepts URIs from two callers: DB::buildReader in EntryVisitMechanism supplies a catalogue KV
    // URI (pool/namespace/oid); StoreFactory during wipe supplies a store namespace URI
    // (pool/namespace). Only pool and namespace are needed here.
    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    ASSERT(parts.size() == 2 || parts.size() == 3);

    pool_ = parts[0];
    db_namespace_ = parts[1];

    readConfig(config, component, false);

    root_kv_.emplace(pool_, root_namespace_, "main_kv");
    db_kv_.emplace(pool_, db_namespace_, "catalogue_kv");
}

void RadosCommon::readConfig(const Config& config, const std::string& component, bool readPool) {

    eckit::LocalConfiguration c{};

    if (config.has("rados")) {
        c = config.getSubConfiguration("rados");
    }

    maxPartSize_ = c.getInt("maxPartSize", 0);

    std::string first_cap{component};
    first_cap[0] = toupper(component[0]);

    std::string all_caps{component};
    for (auto& c : all_caps) {
        c = toupper(c);
    }

    if (readPool) {
        pool_ = "default";
    }
    root_namespace_ = "root";

    if (readPool) {
        pool_ = c.getString("pool", pool_);
        if (c.has(component)) {
            pool_ = c.getSubConfiguration(component).getString("pool", pool_);
        }
    }
    root_namespace_ = c.getString("root_namespace", root_namespace_);
    if (c.has(component)) {
        root_namespace_ = c.getSubConfiguration(component).getString("root_namespace", root_namespace_);
    }

    if (readPool) {
        pool_ = eckit::Resource<std::string>("fdbRados" + first_cap + "Pool;$FDB_RADOS_" + all_caps + "_POOL", pool_);
    }
    root_namespace_ = eckit::Resource<std::string>(
        "fdbRados" + first_cap + "RootNamespace;$FDB_RADOS_" + all_caps + "_ROOT_NAMESPACE", root_namespace_);

    nspace_prefix_ = c.getString("namespace_prefix", nspace_prefix_);
    if (c.has(component)) {
        nspace_prefix_ = c.getSubConfiguration(component).getString("namespace_prefix", nspace_prefix_);
    }
    if (nspace_prefix_.find('_') != std::string::npos) {
        throw eckit::UserError("RADOS namespace_prefix must not contain underscores: '" + nspace_prefix_ + "'", Here());
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
