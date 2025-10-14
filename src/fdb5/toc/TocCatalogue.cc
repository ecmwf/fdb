/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Timer.h"

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
#include <memory>

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


std::unique_ptr<WipeState> TocCatalogue::wipeInit() const {
    return std::make_unique<TocWipeState>(dbKey_, config_);
}

bool TocCatalogue::wipeIndex(const Index& index, bool include, WipeState& wipeState) const {

    eckit::URI locationURI{index.location().uri()};

    // TocWipeState& tocWipeState = dynamic_cast<TocWipeState&>(wipeState);
    TocWipeState& tocWipeState = static_cast<TocWipeState&>(wipeState);

    if (!locationURI.path().dirName().sameAs(basePath())) {
        include = false;
    }

    // Add the index paths to be removed.
    if (include) {
        tocWipeState.indexesToMask_.emplace_back(index);
        tocWipeState.indexPaths_.insert(locationURI);
    }
    else {
        // This will ensure that if only some indexes are to be removed from a file, then
        // they will be masked out but the file not deleted.
        tocWipeState.safePaths_.insert(locationURI);
    }

    return include;
}


void TocCatalogue::addMaskedPaths(std::set<eckit::URI>& maskedDataPath, TocWipeState& tocWipeState) const {

    std::set<std::pair<eckit::URI, Offset>> metadata;
    allMasked(metadata, maskedDataPath);
    for (const auto& entry : metadata) {
        eckit::PathName path = entry.first.path();
        if (path.dirName().sameAs(basePath())) {
            if (path.baseName().asString().substr(0, 4) == "toc.") {
                tocWipeState.subtocPaths_.insert(entry.first);
            }
            else {
                tocWipeState.indexPaths_.insert(entry.first);
            }
        }
    }
}

std::set<eckit::URI> TocCatalogue::wipeFinialise(WipeState& wipeState) const {

    // We wipe everything if there is nothing within safePaths - i.e. there is
    // no data that wasn't matched by the request

    WipeElements& wipeElements  = wipeState.wipeElements();
    TocWipeState& tocWipeState = static_cast<TocWipeState&>(wipeState);

    bool wipeAll = tocWipeState.safePaths_.empty();

    std::set<eckit::URI> maskedDataPaths;

    std::stringstream ss;
    ss << "FDB owner: " << owner();
    wipeElements.push_back(
        std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE_INFO, ss.str(), std::set<eckit::URI>{}));

    std::set<eckit::URI> catalogueURIs;
    std::set<eckit::URI> auxURIs;
    std::set<eckit::URI> indexURIs;

    if (wipeAll) {
        addMaskedPaths(maskedDataPaths, tocWipeState);

        // toc, subtocs
        catalogueURIs.emplace("file", tocPath().path());
        for (const auto& sub : subTocPaths()) {
            catalogueURIs.emplace("file", sub);
        }

        // schema, lockfiles
        auxURIs.emplace("file", schemaPath().path());
        for (const auto& lck : lockfilePaths()) {
            auxURIs.emplace("file", lck);
        }
    }
    else {
        // Ensure we _really_ don't delete these if not wiping everything
        tocWipeState.subtocPaths_.clear();
        tocWipeState.lockfilePaths_.clear();

        for (const auto& p : tocWipeState.safePaths_) {
            tocWipeState.indexPaths_.erase(p);
        }

        std::set<eckit::URI> safeURIs;
        for (const auto& p : tocWipeState.safePaths_) {
            safeURIs.insert(p);
        }
        wipeElements.push_back(std::make_shared<WipeElement>(
            WipeElementType::WIPE_CATALOGUE_SAFE, "Protected files (explicitly untouched):", std::move(safeURIs)));
    }

    // Add index paths to be removed
    for (const auto& i : tocWipeState.indexPaths_) {
        indexURIs.insert(i);
    }

    if (wipeAll) {
        std::set<eckit::URI> unknownURIs;
        std::vector<eckit::PathName> allPathsVector;
        StdDir(basePath()).children(allPathsVector);
        for (const eckit::PathName& uri : allPathsVector) {
            eckit::URI u("file", uri);
            if (catalogueURIs.find(u) == catalogueURIs.end() && auxURIs.find(u) == auxURIs.end() &&
                indexURIs.find(u) == indexURIs.end()) {
                unknownURIs.insert(u);
            }
        }
        wipeElements.push_back(std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE,
                                                              "Toc files to delete:", std::move(catalogueURIs)));
        wipeElements.push_back(std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE_AUX,
                                                              "Control files to delete:", std::move(auxURIs)));
        if (!unknownURIs.empty()) {
            wipeElements.push_back(std::make_shared<WipeElement>(
                WipeElementType::WIPE_UNKNOWN, "Unexpected files present in the catalogue:", std::move(unknownURIs)));
        }
    }
    wipeElements.push_back(
        std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE, "Index files to delete:", std::move(indexURIs)));

    return maskedDataPaths; // In what sense are these "masked"?
}

// wipe stage 2 | only for wipe all.
bool TocCatalogue::doWipe(const std::vector<eckit::URI>& unknownURIs, WipeState& wipeState) const {
    TocWipeState& tocWipeState = static_cast<TocWipeState&>(wipeState);

    for (const auto& path : tocWipeState.cataloguePaths_) {
        if (path.exists()) {
            remove(path, std::cout, std::cout, true);
        }
    }
    for (const auto& uri : unknownURIs) {
        if (uri.path().exists()) {
            remove(uri.path(), std::cout, std::cout, true);
        }
    }

    tocWipeState.cataloguePaths_.clear();
    return true;
}

// wipe stage 1: wipe indexes
bool TocCatalogue::doWipe(WipeState& wipeState) const {
    bool wipeAll = true;

    TocWipeState& tocWipeState = static_cast<TocWipeState&>(wipeState);

    for (const auto& el : tocWipeState.wipeElements()) {
        if (el->type() == WipeElementType::WIPE_CATALOGUE_SAFE && !el->uris().empty()) {
            wipeAll = false;
        }
    }

    for (const auto& el :  tocWipeState.wipeElements()) {
        auto type = el->type();
        if (type == WipeElementType::WIPE_CATALOGUE || type == WipeElementType::WIPE_CATALOGUE_AUX) {
            for (const auto& uri : el->uris()) {
                if (wipeAll) {
                    tocWipeState.cataloguePaths_.insert(uri.path().dirName()); /// @todo: Would be nice if we didn't aggregate here...
                }
                remove(uri.path(), std::cout, std::cout, true);
            }
        }
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
