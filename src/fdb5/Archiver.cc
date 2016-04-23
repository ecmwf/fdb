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

#include "fdb5/MasterConfig.h"
#include "fdb5/ArchiveVisitor.h"
#include "fdb5/AdoptVisitor.h"


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

void Archiver::write(const eckit::DataBlobPtr blob)
{
    NOTIMP; /// @todo this will substitute the GribArchiver
}

void Archiver::write(const Key& key, const void* data, size_t length)
{
    eckit::Log::info() << "Archiver write " << length << std::endl;

    ASSERT(data);

    const Schema& schema = MasterConfig::instance().schema();

    ArchiveVisitor visitor(*this, key, data, length);

    schema.expand(key, visitor);

    if(visitor.rule() == 0) { // Make sure we did find a rule that matched
        std::ostringstream oss;
        oss << "FDB: Could not find a rule to archive " << key;
        throw eckit::SeriousBug(oss.str());
    }
}

void Archiver::adopt(const Key& key, const eckit::PathName& path, eckit::Offset offset, eckit::Length length)
{
    const Schema& schema = MasterConfig::instance().schema();

    AdoptVisitor visitor(*this, key, path, offset, length);

    schema.expand(key, visitor);

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

    if(i != databases_.end() )
        return *(i->second.get());

    eckit::SharedPtr<DB> db ( DBFactory::build(fdbWriterDB_, key) );
    ASSERT(db);
    databases_[key] = db;
    return *db;
}

void Archiver::print(std::ostream& out) const
{
    out << "Archiver("
        << ")"
        << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
