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
#include "fdb5/toc/TocDB.h"
#include "fdb5/toc/TocStats.h"
#include "fdb5/toc/TocPurgeVisitor.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDB::TocDB(const Key& key, const eckit::Configuration& config) :
    DB(key),
    TocHandler(RootManager(config).directory(key), config) {
}

TocDB::TocDB(const eckit::PathName& directory, const eckit::Configuration& config) :
    DB(Key()),
    TocHandler(directory, config) {

    // Read the real DB key into the DB base object
    dbKey_ = databaseKey();
}

TocDB::~TocDB() {
}

void TocDB::axis(const std::string&, eckit::StringSet&) const {
    Log::error() << "axis() not implemented for " << *this << std::endl;
    NOTIMP;
}

bool TocDB::open() {
    Log::error() << "Open not implemented for " << *this << std::endl;
    NOTIMP;
}

bool TocDB::exists() const {
    return TocHandler::exists();
}

void TocDB::archive(const Key&, const void*, Length) {
    Log::error() << "Archive not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::flush() {
    Log::error() << "Flush not implemented for " << *this << std::endl;
    NOTIMP;
}

eckit::DataHandle *TocDB::retrieve(const Key&) const {
    Log::error() << "Retrieve not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::close() {
    Log::error() << "Close not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::dump(std::ostream &out, bool simple) const {
    TocHandler::dump(out, simple);
}

std::string TocDB::owner() const {
    return dbOwner();
}

const Schema& TocDB::schema() const {
    return schema_;
}

eckit::PathName TocDB::basePath() const {
    return directory_;
}

std::vector<PathName> TocDB::metadataPaths() const {

    std::vector<PathName> paths(subTocPaths());

    paths.emplace_back(schemaPath());
    paths.emplace_back(tocPath());

    return paths;
}

void TocDB::visitEntries(EntryVisitor& visitor, bool sorted) {

    std::vector<Index> all = indexes(sorted);

    // Allow the visitor to selectively reject this DB.
    if (visitor.visitDatabase(*this)) {
        if (visitor.visitIndexes()) {
            for (const Index& idx : all) {
                if (visitor.visitEntries()) {
                    idx.entries(visitor); // contains visitIndex
                } else {
                    visitor.visitIndex(idx);
                }
            }
        }
    }

    visitor.databaseComplete(*this);
}

void TocDB::loadSchema() {
    Timer timer("TocDB::loadSchema()", Log::debug<LibFdb5>());
    schema_.load( schemaPath() );
}

DbStats TocDB::statistics() const
{
    return TocHandler::stats();
}

StatsReportVisitor* TocDB::statsReportVisitor() const {
    return new TocStatsReportVisitor(*this);
}

PurgeVisitor *TocDB::purgeVisitor() const {
    return new TocPurgeVisitor(*this);
}

void TocDB::maskIndexEntry(const Index &index) const {
    TocHandler handler(basePath());
    handler.writeClearRecord(index);
}

std::vector<Index> TocDB::indexes(bool sorted) const {
    return loadIndexes(sorted);
}

void TocDB::visit(DBVisitor &visitor) {
    visitor(*this);
}

std::string TocDB::dbType() const
{
    return TocDB::dbTypeName();
}

void TocDB::checkUID() const {
    TocHandler::checkUID();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
