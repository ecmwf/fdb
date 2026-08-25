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
#include "fdb5/rados/RadosCommon.h"

#include "metkit/mars/MarsRequest.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/log/Log.h"
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
    const RadosSpace space = rados_space(config, key);
    return eckit::RadosKeyValue{space.pool, space.databaseNamespace(key), "catalogue_kv"}.uri();
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

    std::vector<eckit::URI> res{};

    for (const auto& space : rados_spaces(config)) {
        eckit::RadosKeyValue rootKv{space.pool, space.rootNamespace, "main_kv"};
        if (!rootKv.exists()) {
            continue;
        }
        for (const auto& key : rootKv.keys()) {
            try {

                std::vector<char> val;
                rootKv.getMemoryStream(val, key, "root kv");

                eckit::URI uri(std::string(val.begin(), val.end()));
                ASSERT(uri.scheme() == typeName());

                eckit::RadosKeyValue db_kv{uri};
                fdb5::Key db_key = read_db_key(db_kv);

                if (matches(db_key)) {
                    eckit::Log::debug<LibFdb5>()
                        << " found match with " << rootKv.uri() << " at key " << key << std::endl;
                    res.push_back(uri);
                }
            }
            catch (eckit::Exception& e) {
                eckit::Log::error() << "Error loading FDB database " << key << " from " << rootKv.uri() << std::endl;
                eckit::Log::error() << e.what() << std::endl;
            }
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

static EngineBuilder<RadosEngine> rados_builder;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
