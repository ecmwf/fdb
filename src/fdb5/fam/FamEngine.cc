/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

#include "fdb5/fam/FamEngine.h"

#include <climits>

#include "eckit/io/fam/FamMap.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Engine.h"
#include "fdb5/fam/FamCatalogue.h"
#include "fdb5/fam/FamCommon.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

eckit::URI parseRootURI(const Config& config) {
    if (config.has("fam_roots")) {
        const auto roots = config.getSubConfigurations("fam_roots");
        if (!roots.empty()) {
            return eckit::URI{roots[0].getString("uri")};
        }
    }
    throw eckit::BadValue("FamEngine: no 'fam_roots' found in config");
}

const EngineBuilder<FamEngine> fam_engine_builder;

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

std::string FamEngine::name() const {
    return FamCommon::type;
}

eckit::URI FamEngine::rootURI(const Config& config) {
    return parseRootURI(config);
}

bool FamEngine::canHandle(const eckit::URI& uri, const Config& config) const {

    if (uri.scheme() != FamCommon::type) {
        return false;
    }

    try {
        const eckit::FamRegionName root{uri};
        return root.exists();
    }
    catch (...) {
        LOG_DEBUG_LIB(LibFdb5) << "FamEngine::canHandle: exception checking URI: " << uri << '\n';
        return false;
    }
}

//----------------------------------------------------------------------------------------------------------------------

std::vector<eckit::URI> FamEngine::visitableLocations(const Key& key, const Config& config) const {

    std::vector<eckit::URI> result;

    try {
        const auto uri = parseRootURI(config);
        const eckit::FamRegionName root{uri};

        if (!root.exists()) {
            return result;
        }

        FamCommon::Map reg_map(FamCommon::registry_keyword, root.lookup());

        if (reg_map.empty()) {
            return result;
        }

        for (const auto& reg : reg_map) {
            try {
                if (const auto db_key = FamCommon::decodeKey(reg.value); db_key.match(key)) {
                    const auto name = FamCatalogue::catalogueName(db_key);
                    result.push_back(root.object(name + FamCommon::table_suffix).uri());
                }
            }
            catch (const eckit::Exception& e) {
                eckit::Log::error() << "FamEngine: error decoding registry entry: " << e.what() << std::endl;
            }
        }
    }
    catch (const eckit::Exception& e) {
        LOG_DEBUG_LIB(LibFdb5) << "FamEngine::visitableLocations(key): " << e.what() << std::endl;
    }

    return result;
}

std::vector<eckit::URI> FamEngine::visitableLocations(const metkit::mars::MarsRequest& request,
                                                      const Config& config) const {

    std::vector<eckit::URI> result;

    try {
        const auto uri = parseRootURI(config);
        const eckit::FamRegionName root{uri};

        if (!root.exists()) {
            return result;
        }

        FamCommon::Map reg_map(FamCommon::registry_keyword, root.lookup());

        if (reg_map.empty()) {
            return result;
        }

        for (const auto& [k, v] : reg_map) {
            try {
                if (const auto key = FamCatalogue::decodeKey(v); key.partialMatch(request)) {
                    const std::string name = FamCatalogue::catalogueName(key);
                    result.push_back(root.object(name + FamCommon::table_suffix).uri());
                }
            }
            catch (const eckit::Exception& e) {
                eckit::Log::error() << "FamEngine: error decoding registry entry: " << e.what() << std::endl;
            }
        }
    }
    catch (const eckit::Exception& e) {
        LOG_DEBUG_LIB(LibFdb5) << "FamEngine::visitableLocations(request): " << e.what() << std::endl;
    }

    return result;
}

//----------------------------------------------------------------------------------------------------------------------

eckit::URI FamEngine::location(const Key& /*key*/, const Config& /*config*/) const {
    NOTIMP;
}
std::string FamEngine::dbType() const {
    NOTIMP;
}
void FamEngine::print(std::ostream& out) const {
    out << "FamEngine[]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
