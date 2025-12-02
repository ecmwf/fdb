/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/database/Archiver.h"

#include <ctime>
#include <mutex>

#include "eckit/config/Resource.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/Callback.h"
#include "fdb5/database/ArchiveVisitor.h"
#include "fdb5/database/BaseArchiveVisitor.h"
#include "fdb5/database/Store.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Archiver::Archiver(const Config& dbConfig, const ArchiveCallback& callback) :
    dbConfig_(dbConfig), db_(nullptr), callback_(callback) {}

Archiver::~Archiver() {
    flush();  // certify that all sessions are flushed before closing them
}

void Archiver::archive(const Key& key, const void* data, size_t len) {
    auto visitor = ArchiveVisitor::create(*this, key, data, len, callback_);
    archive(key, *visitor);
}

void Archiver::archive(const Key& key, BaseArchiveVisitor& visitor) {

    std::lock_guard<std::recursive_mutex> lock(flushMutex_);
    visitor.rule(nullptr);

    dbConfig_.schema().expand(key, visitor);

    const Rule* rule = visitor.rule();
    if (rule == nullptr) {  // Make sure we did find a rule that matched
        std::ostringstream oss;
        oss << "FDB: Could not find a rule to archive " << key;
        throw eckit::SeriousBug(oss.str());
    }
    // validate metadata
    rule->check(key);
}

void Archiver::flushDatabase(Database& db) {
    // flush the store, pass the number of flushed fields to the catalogue
    db.catalogue_->flush(db.store_->flush());
}

void Archiver::flush() {
    std::lock_guard<std::recursive_mutex> lock(flushMutex_);
    for (auto i = databases_.begin(); i != databases_.end(); ++i) {
        flushDatabase(i->second);
    }
}

void Archiver::selectDatabase(const Key& dbKey) {

    auto i = databases_.find(dbKey);

    if (i != databases_.end()) {
        db_             = &(i->second);
        i->second.time_ = ::time(0);
        return;
    }

    static size_t fdbMaxNbDBsOpen = eckit::Resource<size_t>("fdbMaxNbDBsOpen", 64);

    {
        std::lock_guard<std::mutex> cacheLock(cacheMutex_);
        if (databases_.size() >= fdbMaxNbDBsOpen) {
            bool found    = false;
            time_t oldest = ::time(0) + 24 * 60 * 60;
            Key oldK;
            for (auto i = databases_.begin(); i != databases_.end(); ++i) {
                if (i->second.time_ <= oldest) {
                    found  = true;
                    oldK   = i->first;
                    oldest = i->second.time_;
                }
            }
            if (found) {
                // flushing before evicting from cache
                std::lock_guard<std::recursive_mutex> lock(flushMutex_);

                flushDatabase(databases_[oldK]);

                eckit::Log::info() << "Closing database " << *databases_[oldK].catalogue_ << std::endl;
                databases_.erase(oldK);
            }
        }

        std::shared_ptr<CatalogueWriter> cat = CatalogueWriterFactory::instance().build(dbKey, dbConfig_);
        ASSERT(cat);

        // If this database is locked for writing then this is an error
        if (!cat->enabled(ControlIdentifier::Archive)) {
            std::ostringstream ss;
            ss << "Database " << *cat << " matched for archived is LOCKED against archiving";
            throw eckit::UserError(ss.str(), Here());
        }

        db_ = &(databases_[dbKey] = Database{::time(0), cat, cat->buildStore()});
    }
}

void Archiver::print(std::ostream& out) const {
    out << "Archiver["
        << "]" << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
