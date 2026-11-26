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
#include "eckit/io/rados/RadosNamespace.h"
#include "eckit/io/rados/RadosObject.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/serialisation/HandleStream.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/serialisation/Stream.h"
#include "eckit/utils/Regex.h"
#include "eckit/utils/Tokenizer.h"

#include <functional>
#include <optional>
#include <set>
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

std::vector<eckit::LocalConfiguration> spaces_config(const Config& config) {
    if (!config.has("spaces")) {
        throw eckit::UserError("RADOS placement requires at least one spaces[] entry", Here());
    }
    return config.getSubConfigurations("spaces");
}

RadosLocation checked_location(const eckit::URI& uri) {
    const auto location = rados_location(uri);
    if (!location) {
        throw eckit::UserError("Invalid RADOS URI '" + uri.asString() + "'", Here());
    }
    return *location;
}

long max_part_size(const Config& config) {

    const long size = config.has("rados") ? config.getSubConfiguration("rados").getLong("maxPartSize", 0) : 0;

    if (size < 0) {
        throw eckit::UserError("rados.maxPartSize must not be negative", Here());
    }

    return size;
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

bool rados_names::has_prefix(const std::string& name, const std::string& prefix) {
    return name.compare(0, prefix.size(), prefix) == 0;
}

std::vector<char> read_value(const eckit::RadosKeyValue& kv, const std::string& key) {
    auto value = kv.get(key);
    if (!value) {
        // Not logged as an error: callers handle a missing entry.
        throw eckit::NotFound("RADOS key '" + key + "' in key-value " + kv.str(), Here(), true);
    }
    return std::move(*value);
}

std::string read_text(const eckit::RadosKeyValue& kv, const std::string& key) {
    const std::vector<char> data = read_value(kv, key);
    return {data.begin(), data.end()};
}

void write_text(eckit::RadosKeyValue& kv, const std::string& key, const std::string& text) {
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
    const std::vector<char> data = read_value(kv, "key");
    eckit::MemoryStream ms{data.data(), data.size()};
    return fdb5::Key(ms);
}

std::string RadosSpace::databaseNamespace(const Key& key) const {
    return namespacePrefix + "_" + key.valuesToString();
}

std::optional<RadosLocation> rados_location(const eckit::URI& uri) {

    if (uri.scheme() != "rados") {
        return std::nullopt;
    }

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    if (parts.size() != 2 && parts.size() != 3) {
        return std::nullopt;
    }

    return RadosLocation{parts[0], parts[1], parts.size() == 3 ? parts[2] : std::string{}};
}

std::vector<RadosSpace> rados_spaces(const Config& config) {

    std::vector<RadosSpace> spaces;
    for (const auto& space : spaces_config(config)) {
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

    const std::string keyString = key.valuesToString();
    for (const auto& space : spaces_config(config)) {
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

    const RadosLocation location = checked_location(uri);

    for (const auto& space : rados_spaces(config)) {
        if (space.pool == location.pool && rados_names::has_prefix(location.nspace, space.namespacePrefix + "_")) {
            return space;
        }
    }

    throw eckit::UserError("No RADOS placement matches URI " + uri.asString(), Here());
}

//----------------------------------------------------------------------------------------------------------------------

RadosCommon::RadosCommon(const Config& config, const RadosSpace& space, const std::string& dbNamespace) :
    pool_(space.pool),
    db_namespace_(dbNamespace),
    root_kv_(pool_, space.rootNamespace, rados_names::rootKeyValue),
    db_kv_(pool_, db_namespace_, rados_names::catalogueKeyValue),
    maxPartSize_(max_part_size(config)) {}

RadosCommon::RadosCommon(const Config& config, const RadosSpace& space, const Key& key) :
    RadosCommon(config, space, space.databaseNamespace(key)) {}

RadosCommon::RadosCommon(const Config& config, const Key& key) : RadosCommon(config, rados_space(config, key), key) {}

RadosCommon::RadosCommon(const Config& config, const eckit::URI& uri) :
    RadosCommon(config, rados_space(config, uri), checked_location(uri).nspace) {}

bool RadosCommon::ownsURI(const eckit::URI& uri) const {
    const auto location = rados_location(uri);
    return location && location->pool == pool_ && location->nspace == db_namespace_;
}

std::set<eckit::URI> RadosCommon::objectURIs() const {

    std::set<eckit::URI> uris;

    const eckit::RadosNamespace db = dbNamespace();
    if (!db.pool().exists()) {
        return uris;
    }

    for (const auto& object : db.listObjects()) {
        if (!object.isPart()) {
            uris.insert(object.uri());
        }
    }

    return uris;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
