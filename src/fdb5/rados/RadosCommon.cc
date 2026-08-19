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
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/Regex.h"
#include "eckit/utils/Tokenizer.h"

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

RadosSpace space_from_root(const eckit::LocalConfiguration& root) {
    RadosSpace space{root.getString("pool"), root.getString("root_namespace"), root.getString("namespace_prefix")};
    if (space.namespacePrefix.find('_') != std::string::npos) {
        throw eckit::UserError("RADOS namespace_prefix must not contain underscores: '" + space.namespacePrefix + "'",
                               Here());
    }
    return space;
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

fdb5::Key read_db_key(const eckit::RadosKeyValue& db_kv) {
    std::vector<char> data;
    eckit::MemoryStream ms = db_kv.getMemoryStream(data, "key", "DB kv");
    return fdb5::Key(ms);
}

std::string RadosSpace::databaseNamespace(const Key& key) const {
    return namespacePrefix + "_" + key.valuesToString();
}

std::vector<RadosSpace> rados_spaces(const Config& config) {
    if (!config.has("spaces")) {
        throw eckit::UserError("RADOS placement requires at least one spaces[] entry", Here());
    }

    std::vector<RadosSpace> spaces;
    for (const auto& space : config.getSubConfigurations("spaces")) {
        if (!space.has("roots")) {
            throw eckit::UserError("RADOS placement requires roots[] in every spaces[] entry", Here());
        }
        for (const auto& root : space.getSubConfigurations("roots")) {
            spaces.emplace_back(space_from_root(root));
        }
    }
    return spaces;
}

RadosSpace rados_space(const Config& config, const Key& key) {
    if (!config.has("spaces")) {
        throw eckit::UserError("RADOS placement requires at least one spaces[] entry", Here());
    }

    const std::string keyString = key.valuesToString();
    for (const auto& space : config.getSubConfigurations("spaces")) {
        if (!eckit::Regex{space.getString("regex", ".*")}.match(keyString)) {
            continue;
        }
        if (!space.has("roots")) {
            throw eckit::UserError("RADOS placement requires roots[] in matching spaces[] entry", Here());
        }
        const auto roots = space.getSubConfigurations("roots");
        if (roots.size() != 1) {
            throw eckit::UserError("RADOS placement requires exactly one root in matching spaces[] entry", Here());
        }
        return space_from_root(roots.front());
    }

    throw eckit::UserError("No RADOS placement matches database key " + keyString, Here());
}

RadosSpace rados_space(const Config& config, const eckit::URI& uri) {
    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    ASSERT(parts.size() == 2 || parts.size() == 3);

    for (const auto& space : rados_spaces(config)) {
        if (space.pool == parts[0] && parts[1].rfind(space.namespacePrefix + "_", 0) == 0) {
            return space;
        }
    }

    throw eckit::UserError("No RADOS placement matches URI " + uri.asString(), Here());
}

//----------------------------------------------------------------------------------------------------------------------

RadosCommon::RadosCommon(const Config& config, const std::string& component, const Key& key) {

    std::vector<std::string> valid{"catalogue", "store"};
    ASSERT(std::find(valid.begin(), valid.end(), component) != valid.end());

    const RadosSpace space = rados_space(config, key);
    pool_ = space.pool;
    db_namespace_ = space.databaseNamespace(key);
    readConfig(config, component);

    root_kv_.emplace(pool_, space.rootNamespace, "main_kv");
    db_kv_.emplace(pool_, db_namespace_, "catalogue_kv");
}

RadosCommon::RadosCommon(const Config& config, const std::string& component, const eckit::URI& uri) {

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    ASSERT(parts.size() == 2 || parts.size() == 3);

    const RadosSpace space = rados_space(config, uri);
    pool_ = parts[0];
    db_namespace_ = parts[1];
    readConfig(config, component);

    root_kv_.emplace(pool_, space.rootNamespace, "main_kv");
    db_kv_.emplace(pool_, db_namespace_, "catalogue_kv");
}

void RadosCommon::readConfig(const Config& config, const std::string& component) {

    eckit::LocalConfiguration c{};

    if (config.has("rados")) {
        c = config.getSubConfiguration("rados");
    }

    maxPartSize_ = c.getInt("maxPartSize", 0);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
