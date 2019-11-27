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
#include "fdb5/toc/TocPurgeVisitor.h"
#include "fdb5/toc/TocStats.h"
#include "fdb5/toc/TocWipeVisitor.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocCatalogue::TocCatalogue(const Key& key, const fdb5::Config& config) :
    Catalogue(key, config),
    TocHandler(RootManager(config).directory(key), config) {
}

TocCatalogue::TocCatalogue(const eckit::PathName& directory, const fdb5::Config& config) :
    Catalogue(Key(), config),
    TocHandler(directory, config) {

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
    return schema_;
}

const eckit::PathName& TocCatalogue::basePath() const {
    return directory_;
}

std::vector<PathName> TocCatalogue::metadataPaths() const {

    std::vector<PathName> paths(subTocPaths());

    paths.emplace_back(schemaPath());
    paths.emplace_back(tocPath());

    std::vector<PathName>&& lpaths(lockfilePaths());
    paths.insert(paths.end(), lpaths.begin(), lpaths.end());

    return paths;
}

void TocCatalogue::visitEntries(EntryVisitor& visitor, const Store& store, bool sorted) {

    std::vector<Index> all = indexes(sorted);

    // Allow the visitor to selectively reject this DB.
    if (visitor.visitDatabase(*this, store)) {
        if (visitor.visitIndexes()) {
            for (const Index& idx : all) {
                if (visitor.visitEntries()) {
                    idx.entries(visitor); // contains visitIndex
                } else {
                    visitor.visitIndex(idx);
                }
            }
        }

        visitor.catalogueComplete(*this);
    }

}

void TocCatalogue::loadSchema() {
    Timer timer("TocCatalogue::loadSchema()", Log::debug<LibFdb5>());
    schema_.load( schemaPath() );
}

StatsReportVisitor* TocCatalogue::statsReportVisitor() const {
    return new TocStatsReportVisitor(*this);
}

PurgeVisitor *TocCatalogue::purgeVisitor(const Store& store) const {
    return new TocPurgeVisitor(*this, store);
}

WipeVisitor* TocCatalogue::wipeVisitor(const Store& store, const metkit::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const {
    return new TocWipeVisitor(*this, store, request, out, doit, porcelain, unsafeWipeAll);
}

void TocCatalogue::maskIndexEntry(const Index &index) const {
    TocHandler handler(basePath());
    handler.writeClearRecord(index);
}

std::vector<Index> TocCatalogue::indexes(bool sorted) const {
    return loadIndexes(sorted);
}

void TocCatalogue::allMasked(std::set<std::pair<PathName, Offset>>& metadata,
                      std::set<PathName>& data) const {
    enumerateMasked(metadata, data);
}

std::string TocCatalogue::type() const
{
    return TocCatalogue::catalogueTypeName();
}

void TocCatalogue::checkUID() const {
    TocHandler::checkUID();
}

void TocCatalogue::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {
    TocHandler::control(action, identifiers);
}

bool TocCatalogue::retrieveLocked() const {
    return TocHandler::retrieveLocked();
}

bool TocCatalogue::archiveLocked() const {
    return TocHandler::archiveLocked();
}

bool TocCatalogue::listLocked() const {
    return TocHandler::listLocked();
}

bool TocCatalogue::wipeLocked() const {
    return TocHandler::wipeLocked();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
