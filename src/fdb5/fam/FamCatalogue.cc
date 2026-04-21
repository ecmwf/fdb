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
#include <cstddef>
#include <sstream>

#include "eckit/io/fam/FamMap.h"
#include "eckit/io/fam/FamMapEntry.h"
#include "eckit/io/fam/FamPath.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/MD5.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/fam/FamIndex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

/// Number of hex characters taken from the MD5 digest for object names.
///
/// 32 hex chars = 128 bits → birthday bound ~2.7e18 entries before 1% collision.
///
/// OpenFAM limits dataitem names to 40 characters (RadixTree MAX_KEY_LEN).
/// The deepest derived name is created by FamMap bucket lists:
///   prefix(1) + hash(32) + "." + bucket_index(≤4) + sentinel(1) = 39 chars (≤ 40).
constexpr size_t k_hash_length = 32;

static_assert(static_cast<size_t>(2 * MD5_DIGEST_LENGTH) >= k_hash_length,
              "MD5 hex digest (32 chars) must be at least k_hash_length.");

/// Hash a Key to a fixed-length hex string for OpenFAM object names.
std::string hashKey(const Key& key) {
    const auto key_str = FamCommon::toString(key);
    return eckit::MD5(key_str).digest().substr(0, k_hash_length);
}

std::string stripSuffix(const std::string& name, const std::string& suffix) {
    if (name.size() >= suffix.size() && name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0) {
        return name.substr(0, name.size() - suffix.size());
    }
    return name;
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

FamCatalogue::FamCatalogue(const Key& key, const fdb5::Config& config) :
    CatalogueImpl(key, {}, config), FamCommon(key, config), name_{catalogueName(key)} {}

FamCatalogue::FamCatalogue(const eckit::URI& uri, const ControlIdentifiers& control_identifiers,
                           const fdb5::Config& config) :
    CatalogueImpl({}, control_identifiers, config), FamCommon(uri, config) {

    // Strip table_suffix to recover the logical catalogue name
    name_ = stripSuffix(eckit::FamPath(uri).objectName(), FamCommon::table_suffix);

    auto iter = catalogue().find(db_keyword);
    if (iter == catalogue().end()) {
        throw eckit::BadValue("FamCatalogue: DB key not found in catalogue at: " + uri.asString());
    }
    dbKey_ = decodeKey((*iter).value);
}

//----------------------------------------------------------------------------------------------------------------------

std::string FamCatalogue::catalogueName(const Key& key) {
    return FamCommon::catalogue_prefix + hashKey(key);
}

std::string FamCatalogue::indexName(const std::string& cat_name, Key key) {
    // include catalogue name in hash to ensure per-DB isolation
    key.push("catalogue", cat_name);
    return FamCommon::index_prefix + hashKey(key);
}

//----------------------------------------------------------------------------------------------------------------------

std::string FamCatalogue::indexName(const Key& key) const {
    return indexName(name_, key);
}

std::string FamCatalogue::type() const {
    return FamCommon::type;
}

bool FamCatalogue::exists() const {
    return root_.object(name_ + FamCommon::table_suffix).exists();
}

eckit::URI FamCatalogue::uri() const {
    return root_.object(name_ + FamCommon::table_suffix).uri();
}

const Schema& FamCatalogue::schema() const {
    return schema_;
}

const Rule& FamCatalogue::rule() const {
    ASSERT(rule_);
    return *rule_;
}

void FamCatalogue::loadSchema() {
    std::stringstream stream;
    dumpSchema(stream);
    schema_.load(stream, true);
    rule_ = &schema_.matchingRule(dbKey_);
}

bool FamCatalogue::uriBelongs(const eckit::URI& uri) const {
    return root_.uriBelongs(uri);
}

bool FamCatalogue::selectIndex(const Key& key) {
    if (currentIndexKey_ != key) {
        currentIndexKey_ = key;
        return false;
    }
    return true;
}

void FamCatalogue::deselectIndex() {
    currentIndexKey_ = {};
}

std::vector<Index> FamCatalogue::indexes(bool /*sorted*/) const {

    if (!exists()) {
        return {};
    }

    std::vector<Index> result;

    for (const auto& [k, v] : catalogue()) {
        const auto key_name = k.asString();
        // Only process entries registered by FamCatalogueWriter::selectIndex(), which
        // stores them under the "i:" prefix.  All other map entries (e.g. "__fdb__")
        // are administrative and must be skipped.
        if (key_name.size() < 2 || key_name[0] != 'i' || key_name[1] != ':') {
            continue;
        }
        // Decode the stored index Key (with keyword names).
        const auto key = decodeKey(v);
        result.emplace_back(new FamIndex(key, root_, indexName(key), true));
    }

    return result;
}

CatalogueWipeState FamCatalogue::wipeInit() const {
    return dbKey_;
}

//----------------------------------------------------------------------------------------------------------------------

void FamCatalogue::checkUID() const {
    NOTIMP;
}
void FamCatalogue::dump(std::ostream& out, bool simple, const eckit::Configuration& /*conf*/) const {
    out << "FamCatalogue " << dbKey_ << ", uri=" << uri() << '\n';
    for (const auto& index : indexes()) {
        index.dump(out, "  ", simple, !simple);
        out << '\n';
    }
}
StatsReportVisitor* FamCatalogue::statsReportVisitor() const {
    NOTIMP;
}
PurgeVisitor* FamCatalogue::purgeVisitor(const Store& store) const {
    NOTIMP;
}
MoveVisitor* FamCatalogue::moveVisitor(const Store& store, const metkit::mars::MarsRequest& request,
                                       const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const {
    NOTIMP;
}
void FamCatalogue::maskIndexEntries(const std::set<Index>& indexes) const {
    auto& cat = catalogue();
    for (const auto& index : indexes) {
        const auto entry_key = std::string("i:") + toString(index.key());
        cat.erase(entry_key);
    }
}
void FamCatalogue::allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata,
                             std::set<eckit::URI>& data) const {}
void FamCatalogue::control(const ControlAction& /*action*/, const ControlIdentifiers& /*identifiers*/) const {}
bool FamCatalogue::markIndexForWipe(const Index& index, bool include, CatalogueWipeState& wipe_state) const {
    const eckit::URI location_uri = index.location().uri();

    // If the index belongs to a different region, refuse to delete it.
    if (!uriBelongs(location_uri)) {
        include = false;
    }

    if (include) {
        wipe_state.markForMasking(index);
        wipe_state.markForDeletion(WipeElementType::CATALOGUE_INDEX, {location_uri});
    }
    else {
        wipe_state.markAsSafe({location_uri});
    }

    return include;
}

void FamCatalogue::finaliseWipeState(CatalogueWipeState& wipe_state) const {
    // Mark the catalogue-level FAM map table as to be deleted (full wipe) or safe (partial).
    // A "full wipe" is indicated by an empty safeURIs set — i.e. everything matched.
    const eckit::URI cat_uri = uri();
    if (wipe_state.safeURIs().empty()) {
        wipe_state.markForDeletion(WipeElementType::CATALOGUE, {cat_uri});
    }
    else {
        wipe_state.markAsSafe({cat_uri});
    }
}

bool FamCatalogue::doWipeUnknowns(const std::set<eckit::URI>& unknown_uris) const {
    for (const auto& uri : unknown_uris) {
        if (!uriBelongs(uri)) {
            continue;
        }
        try {
            root_.object(eckit::FamPath(uri).objectName()).lookup().deallocate();
        }
        catch (const eckit::NotFound& e) {
            LOG_DEBUG_LIB(LibFdb5) << "FamCatalogue::doWipeUnknowns: object already absent: " << e.what() << '\n';
        }
    }
    return true;
}

bool FamCatalogue::doWipeURIs(const CatalogueWipeState& wipe_state) const {
    const bool wipe_all = wipe_state.safeURIs().empty();

    for (const auto& [type, uris] : wipe_state.deleteMap()) {
        for (const auto& uri : uris) {
            if (!uriBelongs(uri)) {
                continue;
            }
            // Each URI points to a FAM object (table/count/lock of a FamMap, or other data item).
            // Best-effort removal: treat NotFound as benign.
            try {
                const auto obj_name = eckit::FamPath(uri).objectName();
                root_.object(obj_name).lookup().deallocate();

                // A FamMap also creates companion objects (.c count, .l lock);
                // attempt to clean those up if the URI was a table (.t) object.
                if (obj_name.size() > 2 && obj_name.substr(obj_name.size() - 2) == FamCommon::table_suffix) {
                    const auto base = obj_name.substr(0, obj_name.size() - 2);
                    for (const char* suffix : {Map::count_suffix, Map::lock_suffix}) {
                        try {
                            root_.object(base + suffix).lookup().deallocate();
                        }
                        catch (const eckit::NotFound& e) {
                            LOG_DEBUG_LIB(LibFdb5)
                                << "FamCatalogue::doWipeURIs: companion object already absent: " << e.what() << '\n';
                        }
                    }
                }
            }
            catch (const eckit::NotFound& e) {
                LOG_DEBUG_LIB(LibFdb5) << "FamCatalogue::doWipeURIs: object already absent: " << e.what() << '\n';
            }
        }
    }

    if (wipe_all) {
        cleanupEmptyDatabase_ = true;
    }

    return true;
}

void FamCatalogue::doWipeEmptyDatabase() const {
    if (!cleanupEmptyDatabase_) {
        return;
    }

    // Deregister this DB from the global FDB registry map.
    try {
        Map registry(registry_keyword, getRegion());
        registry.erase(toString(dbKey_));
    }
    catch (const eckit::Exception& e) {
        LOG_DEBUG_LIB(LibFdb5) << "FamCatalogue::doWipeEmptyDatabase: registry cleanup failed: " << e.what() << '\n';
    }

    cleanupEmptyDatabase_ = false;
}

bool FamCatalogue::doUnsafeFullWipe() const {
    // Delete the catalogue map and all index maps by clearing their FAM objects.
    try {
        // Clear all index maps first.
        for (const auto& [k, v] : catalogue()) {
            const auto key_name = k.asString();
            if (key_name.size() < 2 || key_name[0] != 'i' || key_name[1] != ':') {
                continue;
            }
            const auto key = decodeKey(v);
            const auto idx_name = indexName(key);
            try {
                Map idx_map(idx_name, getRegion());
                idx_map.clear();
            }
            catch (const eckit::Exception& e) {
                LOG_DEBUG_LIB(LibFdb5) << "FamCatalogue::doUnsafeFullWipe: failed to clear index map '" << idx_name
                                       << "': " << e.what() << '\n';
            }
        }

        // Clear the catalogue map itself.
        catalogue().clear();
    }
    catch (const eckit::Exception& e) {
        LOG_DEBUG_LIB(LibFdb5) << "FamCatalogue::doUnsafeFullWipe: failed to clear catalogue: " << e.what() << '\n';
    }

    // Deregister from the global registry.
    try {
        Map registry(registry_keyword, getRegion());
        registry.erase(toString(dbKey_));
    }
    catch (const eckit::Exception& e) {
        LOG_DEBUG_LIB(LibFdb5) << "FamCatalogue::doUnsafeFullWipe: registry deindex failed: " << e.what() << '\n';
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
