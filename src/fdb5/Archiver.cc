/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>
#include <sstream>

#include "eckit/config/Resource.h"
#include "eckit/io/DataHandle.h"
#include "eckit/log/Timer.h"

#include "marslib/MarsTask.h"

#include "fdb5/Key.h"
#include "fdb5/Archiver.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/WriteVisitor.h"

using namespace eckit;

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

void Archiver::write(const DataBlobPtr blob)
{
    NOTIMP; /// @todo this will substitute the GribArchiver
}

struct ArchiveVisitor : public WriteVisitor {

    Archiver& owner_;

    const Key& field_;
    const void* data_;
    eckit::Length length_;

    ArchiveVisitor(Archiver& owner, const Key& field, const void* data, eckit::Length length) :
        WriteVisitor(owner.prev_),
        owner_(owner),
        field_(field),
        data_(data),
        length_(length)
    {
    }

    virtual bool selectDatabase(const Key& key, const Key& full) {
        Log::info() << "selectDatabase " << key << std::endl;
        owner_.current_ = &owner_.session(key);
        return true;
    }

    virtual bool selectIndex(const Key& key, const Key& full) {
        Log::info() << "selectIndex " << key << std::endl;
        ASSERT(owner_.current_);
        return owner_.current_->selectIndex(key);
    }

    virtual bool selectDatum(const Key& key, const Key& full) {
        Log::info() << "selectDatum " << key << ", " << full << std::endl;
        ASSERT(owner_.current_);
        owner_.current_->archive(key,data_,length_);

        if(1)
        {
            StringSet missing;
            const StringDict& f = field_.dict();
            const StringDict& k = full.dict();

            for(StringDict::const_iterator j = f.begin(); j != f.end(); ++j) {
                if(k.find((*j).first) == k.end()) {
                    missing.insert((*j).first);
                }

                if(missing.size()) {
                    std::ostringstream oss;
                    oss << "Keys not used in archiving: " << missing;
                    throw SeriousBug(oss.str());
                }
            }
        }

        return true;
    }

};

void Archiver::write(const Key& key, const void* data, eckit::Length length)
{
    const Rules& rules = MasterConfig::instance().rules();

    ArchiveVisitor visitor(*this, key, data, length);

    rules.expand(key, visitor);

    if(visitor.count() == 1) { // Make sure we did write something
        std::ostringstream oss;
        oss << "FDB: Could not find a rule to archive " << key;
        throw SeriousBug(oss.str());
    }
}

void Archiver::flush()
{
    for(store_t::iterator i = databases_.begin(); i != databases_.end(); ++i) {
        i->second->flush();
    }
}


DB& Archiver::session(const Key& key)
{
    store_t::iterator i = databases_.find(key);

    if(i != databases_.end() )
        return *(i->second.get());

    eckit::SharedPtr<DB> newSession ( DBFactory::build(fdbWriterDB_, key) );
    ASSERT(newSession);
    databases_[key] = newSession;
    return *newSession;
}

void Archiver::print(std::ostream& out) const
{
    out << "Archiver("
        << ")"
        << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
