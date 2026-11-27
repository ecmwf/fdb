// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @author Metin Cakircali
/// @date   Feb 2024

#pragma once

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"

#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/io/rados/RadosNamespace.h"
#include "eckit/serialisation/Stream.h"

#include <functional>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace fdb5 {

struct RadosSpace {
    std::string pool;
    std::string rootNamespace;
    std::string namespacePrefix;

    std::string databaseNamespace(const Key& key) const;
};

// Entries and objects named after user keys carry one of these prefixes, so that no index or datum key can
// collide with the backend's own metadata entries (`key`, `schema`, `control.*`) or with `catalogue_kv`.
namespace rados_names {

constexpr const char* indexPrefix = "index.";  // catalogue KV entry and index KV object: index.<index key>
constexpr const char* axisPrefix = "axis.";    // index KV marker axis.<axis>; axis KV object axis.<index key>.<axis>
constexpr const char* fieldPrefix = "field.";  // index KV entry: field.<datum key>
constexpr const char* dataPrefix = "data.";    // data object: data.<index key>.<unique digest>

constexpr const char* rootKeyValue = "main_kv";            // registry of the databases of a space
constexpr const char* catalogueKeyValue = "catalogue_kv";  // catalogue of a database

bool has_prefix(const std::string& name, const std::string& prefix);

}  // namespace rados_names

// Reads the value stored under `key`. Throws eckit::NotFound if the KV or the entry is missing.
std::vector<char> read_value(const eckit::RadosKeyValue& kv, const std::string& key);

// Reads the value stored under `key` as text. Throws eckit::NotFound if it is missing.
std::string read_text(const eckit::RadosKeyValue& kv, const std::string& key);
void write_text(eckit::RadosKeyValue& kv, const std::string& key, const std::string& text);

// Returns the bytes `write` puts on a stream, for storing serialised objects as KV values.
std::vector<char> serialise(const std::function<void(eckit::Stream&)>& write);

// Reads the `key` entry of a catalogue or index KV and deserialises it into an fdb5::Key.
// Throws eckit::NotFound if the KV or its `key` entry is missing.
fdb5::Key read_key(const eckit::RadosKeyValue& kv);

// RADOS space is selected from the sole root of the first matching `spaces[]` entry.
RadosSpace rados_space(const Config&, const Key&);
RadosSpace rados_space(const Config&, const eckit::URI&);
std::vector<RadosSpace> rados_spaces(const Config&);

// Components of a `rados:<pool>/<namespace>[/<object>]` URI; `object` is empty for a namespace URI.
struct RadosLocation {
    std::string pool;
    std::string nspace;
    std::string object;
};

// Empty unless `uri` has the rados scheme and two or three components.
std::optional<RadosLocation> rados_location(const eckit::URI& uri);

// State shared by the RADOS store and catalogue of one database: its pool, namespace and KVs.
class RadosCommon {

public:  // methods

    RadosCommon(const Config&, const Key&);
    RadosCommon(const Config&, const eckit::URI&);

protected:  // methods

    eckit::RadosNamespace dbNamespace() const { return {pool_, db_namespace_}; }

    // True for a rados URI of the database namespace or of an object in it.
    bool ownsURI(const eckit::URI& uri) const;

    // URIs of the objects in the database namespace. The parts of a multipart object are not listed;
    // they are handled together with its first object.
    std::set<eckit::URI> objectURIs() const;

private:  // methods

    RadosCommon(const Config&, const RadosSpace&, const Key&);
    RadosCommon(const Config&, const RadosSpace&, const std::string& dbNamespace);

protected:  // members

    std::string pool_;
    std::string db_namespace_;

    eckit::RadosKeyValue root_kv_;
    eckit::RadosKeyValue db_kv_;

    eckit::Length maxPartSize_;
};

}  // namespace fdb5
