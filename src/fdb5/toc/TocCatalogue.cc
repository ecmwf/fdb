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

TocCatalogue::~TocCatalogue() {
}

void TocCatalogue::axis(const std::string&, eckit::StringSet&) const {
    Log::error() << "axis() not implemented for " << *this << std::endl;
    NOTIMP;
}

bool TocCatalogue::open() {
    Log::error() << "Open not implemented for " << *this << std::endl;
    NOTIMP;
}

bool TocCatalogue::exists() const {
    return TocHandler::exists();
}

/*void TocCatalogue::archive(const Key&, const void*, Length) {
    Log::error() << "Archive not implemented for " << *this << std::endl;
    NOTIMP;
}*/

void TocCatalogue::flush() {
    Log::error() << "Flush not implemented for " << *this << std::endl;
    NOTIMP;
}

/*eckit::DataHandle *TocCatalogue::retrieve(const Key&) const {
    Log::error() << "Retrieve not implemented for " << *this << std::endl;
    NOTIMP;
}*/

void TocCatalogue::close() {
    Log::error() << "Close not implemented for " << *this << std::endl;
    NOTIMP;
}

const std::string TocCatalogue::DUMP_PARAM_WALKSUBTOC = "walk";

void TocCatalogue::dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const {
    bool walkSubToc = false;
    conf.get(DUMP_PARAM_WALKSUBTOC, walkSubToc);

    TocHandler::dump(out, simple, walkSubToc);
}

/*std::string TocCatalogue::owner() const {
    return dbOwner();
}*/
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

/*DbStats TocCatalogue::statistics() const
{
    return TocHandler::stats();
}*/

StatsReportVisitor* TocCatalogue::statsReportVisitor() const {
    return new TocStatsReportVisitor(*this);
}

PurgeVisitor *TocCatalogue::purgeVisitor() const {
    return new TocPurgeVisitor(*this);
}

WipeVisitor* TocCatalogue::wipeVisitor(const metkit::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const {
    return new TocWipeVisitor(*this, request, out, doit, porcelain, unsafeWipeAll);
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

/*void TocCatalogue::visit(DBVisitor &visitor) {
    visitor(*this);
}*/

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
