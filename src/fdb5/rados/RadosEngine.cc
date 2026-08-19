/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/rados/RadosEngine.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Engine.h"
#include "fdb5/database/Key.h"

#include "metkit/mars/MarsRequest.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/Tokenizer.h"

#include <cctype>
#include <functional>
#include <ostream>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::string RadosEngine::name() const {
    return RadosEngine::typeName();
}

eckit::URI RadosEngine::location(const Key& key, const Config& config) const {
    readConfig(config, "catalogue", true);
    const std::string db_namespace = nspacePrefix_ + "_" + key.valuesToString();
    return eckit::RadosKeyValue{pool_, db_namespace, "catalogue_kv"}.uri();
}

bool RadosEngine::canHandle(const eckit::URI& uri, const Config&) const {

    if (uri.scheme() != typeName()) {
        return false;
    }

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    if (parts.size() != 2 && parts.size() != 3) {
        return false;
    }

    try {
        return eckit::RadosKeyValue{parts[0], parts[1], "catalogue_kv"}.exists();
    }
    catch (const eckit::Exception& e) {
        eckit::Log::debug<LibFdb5>() << "RadosEngine::canHandle: exception checking URI " << uri << ": " << e.what()
                                     << std::endl;
        return false;
    }
}

std::vector<eckit::URI> RadosEngine::visitableLocations(const std::function<bool(const fdb5::Key&)>& matches,
                                                        const Config& config) const {

    const std::string component = "catalogue";

    readConfig(config, component, true);

    rootKv_.emplace(pool_, rootNamespace_, "main_kv");

    std::vector<eckit::URI> res{};

    if (!rootKv_->exists()) {
        return res;
    }

    for (const auto& k : rootKv_->keys()) {
        try {

            std::vector<char> v;
            rootKv_->getMemoryStream(v, k, "root kv");

            eckit::URI uri(std::string(v.begin(), v.end()));
            ASSERT(uri.scheme() == typeName());

            /// @todo: this deserialisation is also performed in RadosCatalogue(uri, ...). Try to avoid one.
            eckit::RadosKeyValue db_kv{uri};  /// @note: includes exist check
            std::vector<char> data;
            eckit::MemoryStream ms = db_kv.getMemoryStream(data, "key", "DB kv");
            fdb5::Key db_key(ms);

            if (matches(db_key)) {
                eckit::Log::debug<LibFdb5>() << " found match with " << rootKv_->uri() << " at key " << k << std::endl;
                res.push_back(uri);
            }
        }
        catch (eckit::Exception& e) {
            eckit::Log::error() << "Error loading FDB database " << k << " from " << rootKv_->uri() << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }
    }

    return res;
}

std::vector<eckit::URI> RadosEngine::visitableLocations(const Key& key, const Config& config) const {
    return visitableLocations([&key](const fdb5::Key& dbKey) { return dbKey.match(key); }, config);
}

std::vector<eckit::URI> RadosEngine::visitableLocations(const metkit::mars::MarsRequest& request,
                                                        const Config& config) const {
    return visitableLocations([&request](const fdb5::Key& dbKey) { return dbKey.partialMatch(request); }, config);
}

void RadosEngine::readConfig(const fdb5::Config& config, const std::string& component, bool readPool) const {

    eckit::LocalConfiguration c{};

    if (config.has("rados")) {
        c = config.getSubConfiguration("rados");
    }

    std::string first_cap{component};
    first_cap[0] = toupper(component[0]);

    std::string all_caps{component};
    for (auto& c : all_caps) {
        c = toupper(c);
    }

    if (readPool) {
        pool_ = "default";
    }
    rootNamespace_ = "root";

    if (readPool) {
        pool_ = c.getString("pool", pool_);
        if (c.has(component)) {
            pool_ = c.getSubConfiguration(component).getString("pool", pool_);
        }
    }
    rootNamespace_ = c.getString("root_namespace", rootNamespace_);
    if (c.has(component)) {
        rootNamespace_ = c.getSubConfiguration(component).getString("root_namespace", rootNamespace_);
    }

    if (readPool) {
        pool_ = eckit::Resource<std::string>("fdbRados" + first_cap + "Pool;$FDB_RADOS_" + all_caps + "_POOL", pool_);
    }
    rootNamespace_ = eckit::Resource<std::string>(
        "fdbRados" + first_cap + "RootNamespace;$FDB_RADOS_" + all_caps + "_ROOT_NAMESPACE", rootNamespace_);

    nspacePrefix_ = c.getString("namespace_prefix", nspacePrefix_);
    if (c.has(component)) {
        nspacePrefix_ = c.getSubConfiguration(component).getString("namespace_prefix", nspacePrefix_);
    }
    if (nspacePrefix_.find('_') != std::string::npos) {
        throw eckit::UserError("RADOS namespace_prefix must not contain underscores: '" + nspacePrefix_ + "'", Here());
    }
}

static EngineBuilder<RadosEngine> rados_builder;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
