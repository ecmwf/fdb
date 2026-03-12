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

#include "fdb5/fam/FamCatalogue.h"

#include <climits>
#include <fstream>

#include "eckit/io/MemoryHandle.h"
#include "eckit/io/fam/FamMap.h"
#include "eckit/io/fam/FamMapEntry.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/fam/FamIndex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

using Map = eckit::FamMap128;

/// Special map key that stores the serialised DB key (with keyword names).
constexpr const char* KEY_DBKEY = "__dbkey__";

Key deserializeKey(const void* data, std::size_t size) {
    eckit::MemoryStream ms{static_cast<const char*>(data), size};
    return Key{ms};
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

FamCatalogue::FamCatalogue(const Key& key, const fdb5::Config& config) :
    CatalogueImpl(key, ControlIdentifiers{}, config),
    FamCommon(key, config),
    name_(catalogueName(FamCommon::toString(key))) {}

FamCatalogue::FamCatalogue(const eckit::URI& uri, const ControlIdentifiers& control_identifiers,
                           const fdb5::Config& config) :
    CatalogueImpl(Key(), control_identifiers, config), FamCommon(uri, config) {

    // Derive the catalogue map name from the table object in the URI.
    // Convention: uri.path().name() == catalogueMapName + "-map-table"
    const std::string object_name = eckit::FamPath(uri).objectName;
    const std::string_view suffix{FamCommon::table_suffix};
    if (object_name.size() > suffix.size() &&
        object_name.compare(object_name.size() - suffix.size(), suffix.size(), suffix) == 0) {
        name_ = object_name.substr(0, object_name.size() - suffix.size());
    }
    else {
        name_ = object_name;
    }

    // Open catalogue map and read the stored DB key.
    if (!root_.exists()) {
        throw eckit::BadValue("FamCatalogue: root region does not exist: " + uri.asString());
    }

    Map cat_map(name_, root_.lookup());
    auto iter = cat_map.find(Map::key_type{KEY_DBKEY});
    if (iter == cat_map.end()) {
        throw eckit::BadValue("FamCatalogue: DB key not found in catalogue at: " + uri.asString());
    }
    auto dbkey_entry   = *iter;
    const auto& buffer = dbkey_entry.value;
    dbKey_             = deserializeKey(buffer.data(), buffer.size());
}

//----------------------------------------------------------------------------------------------------------------------

std::string FamCatalogue::truncateMapComponent(const std::string& key, std::size_t max_len) {
    if (key.size() > max_len) {
        eckit::Log::warning() << "FamCatalogue: map key '" << key << "' truncated to " << max_len
                              << " chars; distinct keys sharing the same prefix will collide" << '\n';
    }
    return key.substr(0, max_len);
}

std::string FamCatalogue::catalogueName(const std::string& key) {
    return "cat-" + truncateMapComponent(key);
}

std::string FamCatalogue::indexName(const std::string& key) {
    return "idx-" + truncateMapComponent(key);
}

//----------------------------------------------------------------------------------------------------------------------

std::string FamCatalogue::type() const {
    return FamCommon::type;
}

bool FamCatalogue::exists() const {
    // A single object-level lookup implicitly confirms the region also exists;
    // FamObjectName::exists() returns false (not throws) on NotFound at any level.
    return root_.object(name_ + FamCommon::table_suffix).exists();
}

eckit::URI FamCatalogue::uri() const {
    return root_.object(name_ + FamCommon::table_suffix).uri();
}

const Schema& FamCatalogue::schema() const {
    return schema_;
}

const Rule& FamCatalogue::rule() const {
    ASSERT(rule_ != nullptr);
    return *rule_;
}

void FamCatalogue::loadSchema() {
    std::ifstream in(config_.schemaPath());
    if (!in) {
        throw eckit::CantOpenFile(config_.schemaPath());
    }
    schema_.load(in);
    rule_ = &schema_.matchingRule(dbKey_);
}

bool FamCatalogue::uriBelongs(const eckit::URI& uri) const {
    return uri.scheme() == type() && root_.uriBelongs(uri);
}

bool FamCatalogue::selectIndex(const Key& key) {
    if (currentIndexKey_ != key) {
        currentIndexKey_ = key;
        return false;
    }
    return true;
}

void FamCatalogue::deselectIndex() {
    currentIndexKey_ = Key();
}

std::vector<Index> FamCatalogue::indexes(bool /*sorted*/) const {

    if (!exists()) {
        return {};
    }

    Map cat_map(name_, root_.lookup());
    std::vector<Index> result;

    // TODO(metin): the current sentinel convention (skip keys starting with '_') is fragile;
    // any future non-index administrative key that doesn't start with '_' would be
    // misread as an index entry. Consider switching to an explicit index-key prefix
    // (e.g. "i:") and storing all internal metadata under '_'-prefixed keys.
    for (auto iter = cat_map.begin(); iter != cat_map.end(); ++iter) {
        auto entry                 = *iter;
        const std::string key_name = entry.key.asString();

        // Skip special sentinel entries.
        if (key_name.empty() || key_name[0] == '_') {
            continue;
        }

        // Decode the stored index Key (with keyword names).
        const Key key = deserializeKey(entry.value.data(), entry.value.size());

        const std::string map_name = indexName(FamCommon::toString(key));
        result.emplace_back(new FamIndex(key, *this, root_, map_name, /*readAxes=*/true));
    }

    return result;
}

CatalogueWipeState FamCatalogue::wipeInit() const {
    return dbKey_;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
