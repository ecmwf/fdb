/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Timer.h"

#include "fdb5/LibFdb.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocDB.h"
#include "fdb5/toc/TocStats.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDB::TocDB(const Key& key, const eckit::Configuration& config) :
    DB(key),
    TocHandler(RootManager::directory(key), config) {
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

void TocDB::dump(std::ostream &out, bool simple) {
    TocHandler::dump(out, simple);
}

const Schema& TocDB::schema() const {
    return schema_;
}

eckit::PathName TocDB::basePath() const {
    return directory_;
}

void TocDB::visitEntries(EntryVisitor& visitor) {

    std::vector<Index> all = indexes();

    for (std::vector<Index>::const_iterator i = all.begin(); i != all.end(); ++i) {
        i->entries(visitor);
    }
}

void TocDB::loadSchema() {
    Timer timer("TocDB::loadSchema()", Log::debug<LibFdb>());
    schema_.load( schemaPath() );
}

void TocDB::checkSchema(const Key &key) const {
    Timer timer("TocDB::checkSchema()", Log::debug<LibFdb>());
    ASSERT(key.rule());
    schema_.compareTo(key.rule()->schema());
}

DbStats TocDB::statistics() const
{
    return TocHandler::stats();
}

std::vector<Index> TocDB::indexes() const {
    return loadIndexes();
}

void TocDB::visit(DBVisitor &visitor) {
    visitor(*this);
}

std::string TocDB::dbType() const
{
    return TocDB::dbTypeName();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
