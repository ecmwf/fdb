/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/Archiver.h"

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"

#include "fdb5/BaseArchiveVisitor.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/Schema.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


Archiver::Archiver() :
    current_(0)
{
    fdbWriterDB_ = eckit::Resource<std::string>("fdbWriterDB","toc.writer");
}

Archiver::~Archiver()
{
    flush(); // certify that all sessions are flushed before closing them
}

void Archiver::archive(const Key& key, BaseArchiveVisitor& visitor) {

    const Schema& schema = MasterConfig::instance().schema();

    visitor.rule(0);

    try {

        schema.expand(key, visitor);

    } catch(SchemaHasChanged& e) {
        eckit::Log::error() << e.what() << std::endl;
        eckit::Log::error() << "Trying with old schema: " << e.path() << std::endl;

        Schema(e.path()).expand(key, visitor);
    }

    if(visitor.rule() == 0) { // Make sure we did find a rule that matched
        std::ostringstream oss;
        oss << "FDB: Could not find a rule to archive " << key;
        throw eckit::SeriousBug(oss.str());
    }
}

void Archiver::flush()
{
    for(store_t::iterator i = databases_.begin(); i != databases_.end(); ++i) {
        i->second->flush();
    }
}


DB& Archiver::database(const Key& key)
{
    store_t::iterator i = databases_.find(key);

    if(i != databases_.end() ) {
        DB& db = *(i->second.get());
        db.touch();
        return db;
    }

    static size_t fdbMaxNbDBsOpen = eckit::Resource<size_t>("fdbMaxNbDBsOpen", 64);

    if(databases_.size() >= fdbMaxNbDBsOpen) {
        bool found = false;
        time_t oldest = ::time(0) + 24*60*60;
        Key oldK;
        for(store_t::iterator i = databases_.begin(); i != databases_.end(); ++i) {
            DB& db = *(i->second.get());
            if(db.lastAccess() <= oldest) {
                found = true;
                oldK = i->first;
                oldest = db.lastAccess();
            }
        }
        if(found) {
            eckit::Log::info() << "Closing database " << *databases_[oldK] << std::endl;
            databases_.erase(oldK);
        }
    }

    eckit::SharedPtr<DB> db ( DBFactory::build(fdbWriterDB_, key) );
    ASSERT(db);
    databases_[key] = db;
    return *db;
}

void Archiver::print(std::ostream& out) const
{
    out << "Archiver["
        << "]"
        << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
