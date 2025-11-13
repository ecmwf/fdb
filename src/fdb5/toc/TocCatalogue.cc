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

// std::vector<PathName> TocCatalogue::metadataPaths() const {

//     std::vector<PathName> paths(subTocPaths());

//     paths.emplace_back(schemaPath());
//     paths.emplace_back(tocPath());

//     std::vector<PathName>&& lpaths(lockfilePaths());
//     paths.insert(paths.end(), lpaths.begin(), lpaths.end());

//     return paths;
// }

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

void TocCatalogue::maskIndexEntry(const Index& index) const {
    TocHandler handler(basePath(), config_);
    handler.writeClearRecord(index);
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


std::unique_ptr<CatalogueWipeState> TocCatalogue::wipeInit() const {
    return std::make_unique<TocWipeState>(dbKey_);
}

bool TocCatalogue::wipeIndex(const Index& index, bool include, CatalogueWipeState& wipeState) const {

    eckit::URI locationURI{index.location().uri()};

    TocWipeState& tocWipeState = static_cast<TocWipeState&>(wipeState);

    if (!locationURI.path().dirName().sameAs(basePath())) {  // as in the overlay
        include = false;
    }

    // Add the index paths to be removed.
    if (include) {
        tocWipeState.indexesToMask_.emplace_back(
            index);  // XXX: Unused!!! Make sure we have tests that cover index masking.
        tocWipeState.indexURIs_.insert(locationURI);
    }
    else {
        // This will ensure that if only some indexes are to be removed from a file, then
        // they will be masked out but the file not deleted.
        tocWipeState.markAsSafe({locationURI});
    }

    return include;
}

void TocCatalogue::addMaskedPaths(TocWipeState& tocWipeState) const {

    std::set<eckit::URI> maskedDataPaths = {};
    std::set<std::pair<eckit::URI, Offset>> metadata;

    allMasked(metadata, maskedDataPaths);
    for (const auto& entry : metadata) {
        eckit::PathName path = entry.first.path();
        if (path.dirName().sameAs(basePath())) {
            if (path.baseName().asString().substr(0, 4) == "toc.") {
                tocWipeState.subtocPaths_.insert(entry.first);
            }
            else {
                tocWipeState.indexURIs_.insert(entry.first);
            }
        }
    }

    for (const auto& datauri : maskedDataPaths) {
        tocWipeState.include(datauri);
    }
}

void TocCatalogue::wipeFinalise(CatalogueWipeState& wipeState) const {

    // We wipe everything if there is nothing within safePaths - i.e. there is
    // no data that wasn't matched by the request

    TocWipeState& tocWipeState = static_cast<TocWipeState&>(wipeState);

    // Gather masked data paths
    addMaskedPaths(tocWipeState);

    // toc, subtocs
    // We may wish to switch to using an insert function, rather than protected friend access.
    tocWipeState.catalogueURIs_.emplace("file", tocPath().path());
    for (const auto& sub : subTocPaths()) {
        tocWipeState.catalogueURIs_.emplace("file", sub);
    }

    // schema
    tocWipeState.catalogueURIs_.emplace("file", schemaPath().path());

    // lockfiles
    for (const auto& lck : lockfilePaths()) {
        tocWipeState.auxCatalogueURIs_.emplace("file", lck);
    }

    tocWipeState.info_ = "FDB Owner: " + owner();

    // ---- MARK FOR DELETION OR AS SAFE ----

    wipeState.markForDeletion(tocWipeState.indexURIs_);

    bool wipeall = wipeState.safeURIs().empty();
    if (wipeall) {
        wipeState.markForDeletion(tocWipeState.catalogueURIs_);
        wipeState.markForDeletion(tocWipeState.auxCatalogueURIs_);
    }
    else {
        wipeState.markAsSafe(tocWipeState.catalogueURIs_);
        wipeState.markAsSafe(tocWipeState.auxCatalogueURIs_);
    }

    std::vector<eckit::PathName> allPathsVector;
    StdDir(basePath()).children(allPathsVector);
    // XXX: Should really only do this in a wipe all scenario
    for (const eckit::PathName& uri : allPathsVector) {
        // XXX: I think we are very inconsistent about where we use scheme = file vs toc. e.g., the toc itself is scheme
        // file.
        if (!(wipeState.ownsURI(eckit::URI("file", uri)) || wipeState.ownsURI(eckit::URI("toc", uri)))) {
            std::cout << "XXX TocCatalogue::wipeFinalise - unrecognised URI: " << uri << std::endl;
            wipeState.insertUnrecognised(eckit::URI("file", uri));
        }
        else {
            std::cout << "XXX TocCatalogue::wipeFinalise - owned URI: " << uri << std::endl;
        }
    }

    // XXX
    // I think, from this point onwards, we should have a well defined
    // include
    // exclude / safe
    // unrecognised.

    // and there should be:
    // 1. no overlap between these
    // 2. include and safe can no longer change for the cat.
    // 3. the unrecognised can be removed.

    // since there are operations in the indexes that are not rm (e.g. masking), we also need to have these stored.
    // but, this should be separated from the above.

    return;
}

bool TocCatalogue::wipeUnknown(const std::set<eckit::URI>& unknownURIs) const {

    for (const auto& uri : unknownURIs) {
        if (uri.path().exists()) {
            remove(uri.path(), std::cout, std::cout, true);
        }
    }

    return true;
}

// NB: very little in here is toc specific.
bool TocCatalogue::doWipe(const CatalogueWipeState& wipeState) const {

    bool wipeAll = wipeState.safeURIs().empty();  // nothing else in the directory.

    for (auto& uri : wipeState.deleteURIs()) {
        remove(uri.path(), std::cout, std::cout, true);

        if (wipeAll) {  // XXX There should only be one folder... why are we doing this every time?
            emptyDatabases_.emplace(uri.scheme(), uri.path().dirName());
        }
    }

    return true;
}

void TocCatalogue::doWipeEmptyDatabases() const {
    for (const auto& uri : emptyDatabases_) {
        eckit::PathName path = uri.path();
        if (path.exists()) {
            // XXX:
            // none of this is atomic, and at this stage we've removed the lock file. Someone could have started writing
            // to the dir again in the interim. Instead of deleting the lock file, maybe we can atomically mv the
            // directory to a tempdir, and then delete the tempdir?
            remove(path, std::cout, std::cout, true);
        }
    }

    emptyDatabases_.clear();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
