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
#include "fdb5/rules/Rule.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocCatalogue.h"
#include "fdb5/toc/TocMoveVisitor.h"
#include "fdb5/toc/TocPurgeVisitor.h"
#include "fdb5/toc/TocStats.h"
// #include "fdb5/toc/TocWipeVisitor.h"

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

std::vector<PathName> TocCatalogue::metadataPaths() const {

    std::vector<PathName> paths(subTocPaths());

    paths.emplace_back(schemaPath());
    paths.emplace_back(tocPath());

    std::vector<PathName>&& lpaths(lockfilePaths());
    paths.insert(paths.end(), lpaths.begin(), lpaths.end());

    return paths;
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

// WipeVisitor* TocCatalogue::wipeVisitor(const metkit::mars::MarsRequest& request, eckit::Queue<WipeElement>& queue,
//                                        bool doit, bool porcelain, bool unsafeWipeAll) const {
//     return new TocWipeVisitor(*this, request, queue, /*out,*/ doit, porcelain, unsafeWipeAll);
// }

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


bool TocCatalogue::wipeInit() const { 

    ASSERT(subtocPaths_.empty());
    ASSERT(lockfilePaths_.empty());
    ASSERT(indexPaths_.empty());
    ASSERT(safePaths_.empty());
    ASSERT(indexesToMask_.empty());

    return true;  // Explore contained indexes
}

bool TocCatalogue::wipe(const Index& index, bool include) const {

    eckit::LocalPathName location{index.location().uri().path()};

    if (!location.dirName().sameAs(basePath())) {
        include = false;
    }

    std::cout << "TocCatalogue::canWipe: index=" << index << ", include=" << include
              << ", basePath=" << basePath() << std::endl;
    // Add the index paths to be removed.
    if (include) {
        indexesToMask_.emplace_back(index);
        indexPaths_.insert(location);

        std::cout << "TocCatalogue::canWipe: indexPaths=" << indexPaths_ << std::endl;
    }
    else {
        // This will ensure that if only some indexes are to be removed from a file, then
        // they will be masked out but the file not deleted.
        safePaths_.insert(location);

        std::cout << "TocCatalogue::canWipe: safePaths=" << safePaths_ << std::endl;
    }
    
    return include;
}


std::set<eckit::URI> TocCatalogue::addMaskedPaths() {

    std::set<std::pair<eckit::URI, Offset>> metadata;
    std::set<eckit::URI> data;
    catalogue_.allMasked(metadata, data);
    for (const auto& entry : metadata) {
        eckit::PathName path = entry.first.path();
        if (path.dirName().sameAs(catalogue_.basePath())) {
            if (path.baseName().asString().substr(0, 4) == "toc.") {
                subtocPaths_.insert(path);
            }
            else {
                indexPaths_.insert(path);
            }
        }
    }
    return data;  // Return the data paths for further processing
    // for (const auto& uri : data) {
    //     if (store_.uriBelongs(uri)) {
    //         dataPaths_.insert(uri.path());
    //         auto auxPaths = getAuxiliaryPaths(uri);
    //         auxiliaryDataPaths_.insert(auxPaths.begin(), auxPaths.end());
    //     }
    // }
}

void TocCatalogue::addMetadataPaths() {

    // toc, schema
    schemaPath_ = catalogue_.schemaPath().path();
    tocPath_    = catalogue_.tocPath().path();

    // subtocs
    const auto&& subtocs(catalogue_.subTocPaths());
    subtocPaths_.insert(subtocs.begin(), subtocs.end());

    // lockfiles
    const auto&& lockfiles(catalogue_.lockfilePaths());
    lockfilePaths_.insert(lockfiles.begin(), lockfiles.end());
}

bool TocCatalogue::wipeFinish() const {
        // We wipe everything if there is nothingn within safePaths - i.e. there is
    // no data that wasn't matched by the request

    bool wipeAll = safePaths_.empty();

    if (wipeAll) {
        addMaskedPaths();
        addMetadataPaths();
    }
    else {
        // Ensure we _really_ don't delete these if not wiping everything
        subtocPaths_.clear();
        lockfilePaths_.clear();
        tocPath_    = "";
        schemaPath_ = "";
    }

}

bool TocCatalogue::doWipe() { return true; }

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
