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
#include "fdb5/toc/TocWipeVisitor.h"

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
    rule_ = &schema_->matchingRule(dbKey_);
}

StatsReportVisitor* TocCatalogue::statsReportVisitor() const {
    return new TocStatsReportVisitor(*this);
}

PurgeVisitor* TocCatalogue::purgeVisitor(const Store& store) const {
    return new TocPurgeVisitor(*this, store);
}

WipeVisitor* TocCatalogue::wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out,
                                       bool doit, bool porcelain, bool unsafeWipeAll) const {
    return new TocWipeVisitor(*this, store, request, out, doit, porcelain, unsafeWipeAll);
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
        if (doit) {
            path.rmdir(false);
        }
    }
    else {
        logVerbose << "Unlinking: ";
        logAlways << path << std::endl;
        if (doit) {
            path.unlink(false);
        }
    }
}

void TocCatalogue::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {
    TocHandler::control(action, identifiers);
}

bool TocCatalogue::enabled(const ControlIdentifier& controlIdentifier) const {
    return CatalogueImpl::enabled(controlIdentifier) && TocHandler::enabled(controlIdentifier);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
