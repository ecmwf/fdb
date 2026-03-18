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
#include <sstream>

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

/// Truncate a fam-name component so that the total FAM object name (including
/// suffixes like "-table" or "-b1023") stays within OpenFAM limits (~40 chars).
std::string truncateKey(const Key& key, const std::size_t max_len = 26) {
    const auto key_str = FamCommon::toString(key);
    if (key_str.size() > max_len) {
        eckit::Log::warning() << "FamCatalogue: map key '" << key_str << "' truncated to " << max_len
                              << " chars; distinct keys sharing the same prefix will collide" << '\n';
    }
    return key_str.substr(0, max_len);
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

FamCatalogue::FamCatalogue(const Key& key, const fdb5::Config& config) :
    CatalogueImpl(key, {}, config), FamCommon(key, config), name_{catalogueName(key)} {}

FamCatalogue::FamCatalogue(const eckit::URI& uri, const ControlIdentifiers& control_identifiers,
                           const fdb5::Config& config) :
    CatalogueImpl({}, control_identifiers, config), FamCommon(uri, config), name_{eckit::FamPath(uri).objectName} {

    // std::cerr << "FamCatalogue: opened catalogue at URI: " << eckit::FamPath(uri).objectName << std::endl;

    // name_ = root_.objectName(uri);
    // , name_{eckit::FamPath(uri).objectName}

    auto iter = catalogue().find(db_keyword);
    if (iter == catalogue().end()) {
        throw eckit::BadValue("FamCatalogue: DB key not found in catalogue at: " + uri.asString());
    }
    dbKey_ = decodeKey((*iter).value);
}

//----------------------------------------------------------------------------------------------------------------------

std::string FamCatalogue::catalogueName(const Key& key) {
    return "cat-" + truncateKey(key);
}

std::string FamCatalogue::indexName(const Key& key) {
    return "idx-" + truncateKey(key);
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
void FamCatalogue::dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const {
    NOTIMP;
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
