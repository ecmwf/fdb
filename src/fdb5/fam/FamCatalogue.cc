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
#include <sstream>

#include "eckit/io/fam/FamMap.h"
#include "eckit/io/fam/FamMapEntry.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/MD5.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/fam/FamIndex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

constexpr size_t k_hash_length = 8;

static_assert(MD5_DIGEST_LENGTH >= k_hash_length,
              "MD5 digest length must be at least 8 bytes for the hashKey() function to produce 16-hex-char output.");

/// Hash a Key to a short, deterministic, fixed-length hex string for OpenFAM object names.
/// Name:  prefix "cat-"/"idx-" (4) + hash (16) + bucket suffix (worst case=13)
/// "-b1023-list-e" (13) = 33 chars, within the ~40-char OpenFAM limit.
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
    name_ = stripSuffix(eckit::FamPath(uri).objectName, FamCommon::table_suffix);

    auto iter = catalogue().find(db_keyword);
    if (iter == catalogue().end()) {
        throw eckit::BadValue("FamCatalogue: DB key not found in catalogue at: " + uri.asString());
    }
    dbKey_ = decodeKey((*iter).value);
}

//----------------------------------------------------------------------------------------------------------------------

std::string FamCatalogue::catalogueName(const Key& key) {
    return "cat-" + hashKey(key);
}

std::string FamCatalogue::indexName(const Key& key) {
    return "idx-" + hashKey(key);
}

//----------------------------------------------------------------------------------------------------------------------

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
void FamCatalogue::maskIndexEntries(const std::set<Index>& indexes) const {}
void FamCatalogue::allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata,
                             std::set<eckit::URI>& data) const {}
void FamCatalogue::control(const ControlAction& /*action*/, const ControlIdentifiers& /*identifiers*/) const {}
bool FamCatalogue::markIndexForWipe(const Index& /*index*/, bool /*include*/,
                                    CatalogueWipeState& /*wipe_state*/) const {
    return false;
}

void FamCatalogue::finaliseWipeState(CatalogueWipeState& wipeState) const {
    // Mark the catalogue-level FAM map table as to be deleted (full wipe) or safe (partial).
    // A "full wipe" is indicated by an empty safeURIs set — i.e. everything matched.
    const eckit::URI cat_uri = uri();
    if (wipeState.safeURIs().empty()) {
        wipeState.markForDeletion(WipeElementType::CATALOGUE, {cat_uri});
    }
    else {
        wipeState.markAsSafe({cat_uri});
    }
}

bool FamCatalogue::doWipeUnknowns(const std::set<eckit::URI>& /*unknown_uris*/) const {
    return false;  // FAM wipe of unknown objects not yet implemented
}

bool FamCatalogue::doWipeURIs(const CatalogueWipeState& /*wipe_state*/) const {
    return false;  // FAM catalogue wipe not yet implemented
}

void FamCatalogue::doWipeEmptyDatabase() const {}

bool FamCatalogue::doUnsafeFullWipe() const {
    return false;  // FAM unsafe full wipe not yet implemented
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
