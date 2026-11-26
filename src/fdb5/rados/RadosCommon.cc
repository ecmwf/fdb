// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/rados/RadosCommon.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/serialisation/HandleStream.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/serialisation/Stream.h"
#include "eckit/utils/Regex.h"
#include "eckit/utils/Tokenizer.h"

#include <functional>
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

bool rados_names::has_prefix(const std::string& name, const std::string& prefix) {
    return name.compare(0, prefix.size(), prefix) == 0;
}

std::string read_text(const eckit::RadosKeyValue& kv, const std::string& key) {
    std::vector<char> data;
    kv.getMemoryStream(data, key, kv.name());
    return {data.begin(), data.end()};
}

void write_text(const eckit::RadosKeyValue& kv, const std::string& key, const std::string& text) {
    kv.put(key, text.data(), text.size());
}

std::vector<char> serialise(const std::function<void(eckit::Stream&)>& write) {
    eckit::MemoryHandle h;
    eckit::HandleStream hs{h};
    h.openForWrite(eckit::Length(0));
    {
        eckit::AutoClose closer(h);
        write(hs);
    }
    const char* bytes = static_cast<const char*>(h.data());
    return {bytes, bytes + hs.bytesWritten()};
}

fdb5::Key read_key(const eckit::RadosKeyValue& kv) {
    std::vector<char> data;
    eckit::MemoryStream ms = kv.getMemoryStream(data, "key", kv.name());
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

RadosCommon::RadosCommon(const Config& config, const Key& key) {

    const RadosSpace space = rados_space(config, key);
    pool_ = space.pool;
    db_namespace_ = space.databaseNamespace(key);
    readConfig(config);

    root_kv_.emplace(pool_, space.rootNamespace, "main_kv");
    db_kv_.emplace(pool_, db_namespace_, "catalogue_kv");
}

RadosCommon::RadosCommon(const Config& config, const eckit::URI& uri) {

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    ASSERT(parts.size() == 2 || parts.size() == 3);

    const RadosSpace space = rados_space(config, uri);
    pool_ = parts[0];
    db_namespace_ = parts[1];
    readConfig(config);

    root_kv_.emplace(pool_, space.rootNamespace, "main_kv");
    db_kv_.emplace(pool_, db_namespace_, "catalogue_kv");
}

void RadosCommon::readConfig(const Config& config) {

    long maxPartSize = 0;

    if (config.has("rados")) {
        maxPartSize = config.getSubConfiguration("rados").getLong("maxPartSize", 0);
    }

    if (maxPartSize < 0) {
        throw eckit::UserError("rados.maxPartSize must not be negative", Here());
    }

    maxPartSize_ = maxPartSize;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
