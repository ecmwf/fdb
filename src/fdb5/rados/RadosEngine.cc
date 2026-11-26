// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0


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
    return eckit::RadosKeyValue{space.pool, space.databaseNamespace(key), rados_names::catalogueKeyValue}.uri();
}

bool RadosEngine::canHandle(const eckit::URI& uri, const Config&) const {

    const auto location = rados_location(uri);
    if (!location) {
        return false;
    }

    try {
        return eckit::RadosKeyValue{location->pool, location->nspace, rados_names::catalogueKeyValue}.exists();
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
        eckit::RadosKeyValue rootKv{space.pool, space.rootNamespace, rados_names::rootKeyValue};
        if (!rootKv.exists()) {
            continue;
        }
        for (const auto& key : rootKv.keys()) {
            try {

                const eckit::URI uri{read_text(rootKv, key)};
                ASSERT(uri.scheme() == typeName());

                eckit::RadosKeyValue db_kv{uri};
                fdb5::Key db_key = read_key(db_kv);

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
