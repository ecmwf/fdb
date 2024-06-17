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

#include "eckit/config/Resource.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/ArchiveVisitor.h"
#include "fdb5/database/BaseArchiveVisitor.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/rules/Rule.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


Archiver::Archiver(const Config& dbConfig) :
    dbConfig_(dbConfig),
    current_(nullptr) {
}

Archiver::~Archiver() {

    flush(); // certify that all sessions are flushed before closing them

    databases_.clear(); //< explicitly delete the DBs before schemas are destroyed
}

void Archiver::archive(const CanonicalKey& key, const void* data, size_t len) {
    ArchiveVisitor visitor(*this, key, data, len);
    archive(key, visitor);
}

void Archiver::archive(const CanonicalKey& key, BaseArchiveVisitor& visitor) {

    visitor.rule(nullptr);
    

    dbConfig_.schema().expand(key, visitor);

    const Rule* rule = visitor.rule();
    if (rule == nullptr) { // Make sure we did find a rule that matched
        std::ostringstream oss;
        oss << "FDB: Could not find a rule to archive " << key;
        throw eckit::SeriousBug(oss.str());
    }
    // validate metadata
    rule->check(key);
}

void Archiver::flush() {
    for (store_t::iterator i = databases_.begin(); i != databases_.end(); ++i) {
        i->second.second->flush();
    }
}


DB& Archiver::database(const CanonicalKey& key) {

    store_t::iterator i = databases_.find(key);

    if (i != databases_.end() ) {
        DB& db = *(i->second.second);
        i->second.first = ::time(0);
        return db;
    }

    static size_t fdbMaxNbDBsOpen = eckit::Resource<size_t>("fdbMaxNbDBsOpen", 64);

    if (databases_.size() >= fdbMaxNbDBsOpen) {
        bool found = false;
        time_t oldest = ::time(0) + 24 * 60 * 60;
        CanonicalKey oldK;
        for (store_t::iterator i = databases_.begin(); i != databases_.end(); ++i) {
            if (i->second.first <= oldest) {
                found = true;
                oldK = i->first;
                oldest = i->second.first;
            }
        }
        if (found) {
            eckit::Log::info() << "Closing database " << *databases_[oldK].second << std::endl;
            databases_.erase(oldK);
        }
    }

    std::unique_ptr<DB> db = DB::buildWriter(key, dbConfig_);

    ASSERT(db);

    // If this database is locked for writing then this is an error
    if (!db->enabled(ControlIdentifier::Archive)) {
        std::ostringstream ss;
        ss << "Database " << *db << " matched for archived is LOCKED against archiving";
        throw eckit::UserError(ss.str(), Here());
    }

    DB& out = *db;
    databases_[key] = std::make_pair(::time(0), std::move(db));
    return out;
}

void Archiver::print(std::ostream &out) const {
    out << "Archiver["
        << "]"
        << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
