/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Timer.h"

#include "fdb5/config/MasterConfig.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocDB.h"
#include "fdb5/toc/TocStatistics.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDB::TocDB(const Key& key) :
    DB(key),
    TocHandler(RootManager::directory(key)) {
}

TocDB::TocDB(const eckit::PathName& directory) :
    DB(Key()),
    TocHandler(directory) {

    // Read the real DB key into the DB base object
    dbKey_ = databaseKey();
}

TocDB::~TocDB() {
}

void TocDB::axis(const std::string &keyword, eckit::StringSet &s) const {
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

void TocDB::archive(const Key &key, const void *data, Length length) {
    Log::error() << "Archive not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::flush() {
    Log::error() << "Flush not implemented for " << *this << std::endl;
    NOTIMP;
}

eckit::DataHandle *TocDB::retrieve(const Key &key) const {
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

    std::vector<Index*> indexes = loadIndexes();

    for (std::vector<Index*>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {
        (*i)->entries(visitor);
    }

    freeIndexes(indexes);
}

void TocDB::loadSchema() {
    Timer timer("TocDB::loadSchema()");
    schema_.load( schemaPath() );
}

void TocDB::checkSchema(const Key &key) const {
    Timer timer("TocDB::checkSchema()");
    ASSERT(key.rule());
    schema_.compareTo(key.rule()->schema());
}

DbStatistics* TocDB::update() const
{
    TocStatistics* stats = new TocStatistics();

    stats->tocRecordsCount_ += numberOfRecords();
    stats->tocFileSize_     += tocPath().size();
    stats->schemaFileSize_  += schemaPath().size();

    return stats;
}

void TocDB::visit(DBVisitor &visitor) {
    visitor(*this);
}

//----------------------------------------------------------------------------------------------------------------------

void TocDB::matchKeyToDB(const Key& key, std::set<Key>& keys)
{
    const Schema& schema = MasterConfig::instance().schema();
    schema.matchFirstLevel(key, keys);
}

std::vector<eckit::PathName> TocDB::databases(const Key &key, const std::vector<eckit::PathName>& dirs) {

    std::set<Key> keys;

    matchKeyToDB(key,keys);

    std::vector<eckit::PathName> result;
    std::set<eckit::PathName> seen;

    for (std::vector<eckit::PathName>::const_iterator j = dirs.begin(); j != dirs.end(); ++j) {

        std::vector<eckit::PathName> subdirs;
        eckit::PathName::match((*j) / "*:*", subdirs, false);

        for (std::set<Key>::const_iterator i = keys.begin(); i != keys.end(); ++i) {

            Regex re("^" + (*i).valuesToString() + "$");

            for (std::vector<eckit::PathName>::const_iterator k = subdirs.begin(); k != subdirs.end(); ++k) {

                if(seen.find(*k) != seen.end()) {
                    continue;
                }

                if (re.match((*k).baseName())) {
                    try {
                        TocHandler toc(*k);
                        if (toc.databaseKey().match(key)) {
                            result.push_back(*k);
                        }
                    } catch (eckit::Exception& e) {
                        eckit::Log::error() <<  "Error loading FDB database from " << *k << std::endl;
                        eckit::Log::error() << e.what() << std::endl;
                    }
                    seen.insert(*k);;
                }

            }
        }
    }

    return result;
}

std::vector<eckit::PathName> TocDB::allDatabases(const Key &key) {
   return databases(key, RootManager::allRoots(key));
}

std::vector<eckit::PathName> TocDB::writableDatabases(const Key &key) {
   return databases(key, RootManager::writableRoots(key));
}

std::vector<eckit::PathName> TocDB::visitableDatabases(const Key &key) {
   return databases(key, RootManager::visitableRoots(key));
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
