/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/filesystem/PathName.h"
#include "eckit/log/Timer.h"

#include <memory>
#include <set>
#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocCatalogue.h"
#include "fdb5/toc/TocMoveVisitor.h"
#include "fdb5/toc/TocPurgeVisitor.h"
#include "fdb5/toc/TocStats.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocCatalogue::TocCatalogue(const Key& key, const fdb5::Config& config) :
    TocCatalogue(key, CatalogueRootManager(config).directory(key), config) {}

TocCatalogue::TocCatalogue(const Key& key, const TocPath& tocPath, const fdb5::Config& config) :
    CatalogueImpl(key, tocPath.controlIdentifiers_, config), TocHandler(tocPath.directory_, config) {}

TocCatalogue::TocCatalogue(const eckit::PathName& directory, const ControlIdentifiers& controlIdentifiers,
                           const fdb5::Config& config) :
    CatalogueImpl(Key(), controlIdentifiers, config), TocHandler(directory, config) {
    // Read the real DB key into the DB base object
    dbKey_ = databaseKey();
}

bool TocCatalogue::exists() const {
    return TocHandler::exists();
}

const std::string TocCatalogue::DUMP_PARAM_WALKSUBTOC = "walk";

void TocCatalogue::dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const {
    bool walkSubToc = false;
    conf.get(DUMP_PARAM_WALKSUBTOC, walkSubToc);

    TocHandler::dump(out, simple, walkSubToc);
}

eckit::URI TocCatalogue::uri() const {
    return eckit::URI(TocEngine::typeName(), basePath());
}

const Schema& TocCatalogue::schema() const {
    ASSERT(schema_);
    return *schema_;
}

const Rule& TocCatalogue::rule() const {
    ASSERT(rule_);
    return *rule_;
}

void TocCatalogue::loadSchema() {
    Timer timer("TocCatalogue::loadSchema()", Log::debug<LibFdb5>());
    schema_ = &SchemaRegistry::instance().get(schemaPath());
    rule_   = &schema_->matchingRule(dbKey_);
}

StatsReportVisitor* TocCatalogue::statsReportVisitor() const {
    return new TocStatsReportVisitor(*this);
}

PurgeVisitor* TocCatalogue::purgeVisitor(const Store& store) const {
    return new TocPurgeVisitor(*this, store);
}

MoveVisitor* TocCatalogue::moveVisitor(const Store& store, const metkit::mars::MarsRequest& request,
                                       const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const {
    return new TocMoveVisitor(*this, store, request, dest, queue);
}

void TocCatalogue::maskIndexEntries(const std::set<Index>& indexes) const {
    TocHandler handler(basePath(), config_);
    for (const auto& index : indexes) {
        handler.writeClearRecord(index);
    }
}

std::vector<Index> TocCatalogue::indexes(bool sorted) const {
    return loadIndexes(sorted);
}

void TocCatalogue::allMasked(std::set<std::pair<URI, Offset>>& metadata, std::set<URI>& data) const {
    enumerateMasked(metadata, data);
}

std::string TocCatalogue::type() const {
    return TocEngine::typeName();
}

void TocCatalogue::checkUID() const {
    TocHandler::checkUID();
}

void TocCatalogue::remove(const eckit::PathName& path, std::ostream& logAlways, std::ostream& logVerbose, bool doit) {
    if (path.isDir()) {
        logVerbose << "rmdir: ";
        logAlways << path << std::endl;
        if (doit)
            path.rmdir(false);
    }
    else {
        logVerbose << "Unlinking: ";
        logAlways << path << std::endl;
        if (doit)
            path.unlink(false);
    }
}

void TocCatalogue::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {
    TocHandler::control(action, identifiers);
}

bool TocCatalogue::enabled(const ControlIdentifier& controlIdentifier) const {
    return CatalogueImpl::enabled(controlIdentifier) && TocHandler::enabled(controlIdentifier);
}


CatalogueWipeState TocCatalogue::wipeInit() const {
    return CatalogueWipeState(dbKey_);
}

bool TocCatalogue::markIndexForWipe(const Index& index, bool include, CatalogueWipeState& wipeState) const {

    eckit::URI locationURI{index.location().uri()};

    if (!locationURI.path().dirName().sameAs(basePath())) {  // as in the overlay
        include = false;
    }

    // Add the index paths to be removed.
    if (include) {
        wipeState.markForMasking(index);
        wipeState.markForDeletion(WipeElementType::CATALOGUE_INDEX, locationURI);
    }
    else {
        // This will ensure that if only some indexes are to be removed from a file, then
        // they will be masked out but the file not deleted.
        // @todo: Is the above comment correct? Where does this masking happen?
        wipeState.markAsSafe({locationURI});
    }

    return include;
}

bool TocCatalogue::uriBelongs(const eckit::URI& uri) const {
    if (!uri.path().dirName().sameAs(basePath())) {
        return false;
    }

    if (uri.scheme() == "toc") {
        return true;
    }

    // Any scheme other than  "toc" or "file" cannot be ours
    if (uri.scheme() != "file") {
        return false;
    }

    // Is it a toc/subtoc, schema or .index file?
    eckit::PathName basename = uri.path().baseName();

    if (basename.extension() == ".index" || basename.asString().substr(0, 3) == "toc" || basename == "schema") {
        return true;
    }

    // Check against lock files
    for (const auto& lck : lockfilePaths()) {
        if (basename == lck.baseName()) {
            return true;
        }
    }

    return false;
}

void TocCatalogue::addMaskedPaths(CatalogueWipeState& wipeState) const {

    std::set<eckit::URI> maskedDataURIs = {};
    std::set<std::pair<eckit::URI, Offset>> metadata;


    /// @note: "allMasked" function interacts unhelpfully with subtocs. If a subtoc is masked, it seems to assume that
    /// all of its indexes are also masked and can be deleted. This is not generally true. Not a problem since
    /// wipeState.markForDeletion will skip any URIs that are already marked as safe, but I find this behaviour a bit
    /// surprising.
    allMasked(metadata, maskedDataURIs);

    for (const auto& entry : metadata) {
        eckit::PathName path = entry.first.path();
        if (path.dirName().sameAs(basePath())) {
            if (path.baseName().asString().substr(0, 4) == "toc.") {
                wipeState.markForDeletion(WipeElementType::CATALOGUE, entry.first);
            }
            else {
                wipeState.markForDeletion(WipeElementType::CATALOGUE_INDEX, entry.first);
            }
        }
    }

    for (const auto& datauri : maskedDataURIs) {
        wipeState.includeData(datauri);
    }
}

void TocCatalogue::finaliseWipeState(CatalogueWipeState& wipeState) const {

    // We wipe everything if there is nothing within safePaths - i.e. there is
    // no data that wasn't matched by the request

    // Gather masked data paths
    addMaskedPaths(wipeState);

    // toc, subtocs
    std::set<eckit::URI> catalogueURIs;
    std::set<eckit::URI> controlURIs;
    catalogueURIs.emplace("file", tocPath().path());
    for (const auto& sub : subTocPaths()) {
        catalogueURIs.emplace("file", sub);
    }

    // schema
    catalogueURIs.emplace("file", schemaPath().path());

    // lockfiles
    for (const auto& lck : lockfilePaths()) {
        controlURIs.emplace("file", lck);
    }

    wipeState.setInfo("FDB Owner: " + owner());

    // ---- MARK FOR DELETION OR AS SAFE ----

    bool wipeall = wipeState.safeURIs().empty();
    if (wipeall) {
        wipeState.markForDeletion(WipeElementType::CATALOGUE, catalogueURIs);
        wipeState.markForDeletion(WipeElementType::CATALOGUE_CONTROL, controlURIs);
    }
    else {
        wipeState.markAsSafe(catalogueURIs);
        wipeState.markAsSafe(controlURIs);
    }

    if (!wipeall) {
        return;
    }

    // Find any files unaccounted for.
    std::vector<eckit::PathName> allPathsVector;
    StdDir(basePath()).children(allPathsVector);

    for (const eckit::PathName& path : allPathsVector) {
        auto uri = eckit::URI("file", path);
        if (!(wipeState.isMarkedForDeletion(uri))) {
            wipeState.insertUnrecognised(uri);
        }
    }

    return;
}

bool TocCatalogue::doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const {

    for (const auto& uri : unknownURIs) {
        if (uri.path().exists()) {
            remove(uri.path(), std::cout, std::cout, true);
        }
    }

    return true;
}

// NB: very little in here is toc specific.
bool TocCatalogue::doWipeURIs(const CatalogueWipeState& wipeState) const {

    bool wipeAll = wipeState.safeURIs().empty();  // nothing else in the directory.

    for (const auto& [type, uris] : wipeState.deleteMap()) {

        for (auto& uri : uris) {
            remove(uri.path(), std::cout, std::cout, true);
        }
    }

    // Grab the database URI from the first uri in the wipeState
    if (wipeAll && !wipeState.deleteMap().empty()) {
        cleanupEmptyDatabase_ = true;
    }

    return true;
}

void TocCatalogue::doWipeEmptyDatabase() const {

    if (cleanupEmptyDatabase_ == false) {
        return;
    }

    eckit::PathName path = uri().path();
    if (path.exists()) {
        remove(path, std::cout, std::cout, true);
    }
    cleanupEmptyDatabase_ = false;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
