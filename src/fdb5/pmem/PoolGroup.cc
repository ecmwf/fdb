/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/pmem/PoolGroup.h"

#include "eckit/filesystem/FileSpaceStrategies.h"
#include "eckit/exception/Exceptions.h"

#include "fdb5/database/Key.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

PoolGroup::PoolGroup(const std::string& name,
                     const std::string& re,
                     const std::string& handler,
                     const std::vector<PoolEntry>& pools) :
    name_(name),
    handler_(handler),
    re_(re),
    pools_(pools) {
}

eckit::PathName PoolGroup::filesystem(const Key& key) const
{
    /// TEMPORARY
    /// We return the first entry in the PoolGroup
    /// We must understand what it means to manage groups of pools

    ASSERT(pools_.size());

    return pools_.front().path();
}

std::vector<eckit::PathName> PoolGroup::writable() const
{
    std::vector<eckit::PathName> result;
    for (PoolVec::const_iterator i = pools_.begin(); i != pools_.end() ; ++i) {
        if(i->writable()) {
            result.push_back(i->path());
        }
    }
    return result;
}

std::vector<eckit::PathName> PoolGroup::visitable() const
{
    std::vector<eckit::PathName> result;
    for (PoolVec::const_iterator i = pools_.begin(); i != pools_.end() ; ++i) {
        if(i->visit()) {
            result.push_back(i->path());
        }
    }
    return result;
}

void PoolGroup::all(eckit::StringSet& pools) const
{
    for (PoolVec::const_iterator i = pools_.begin(); i != pools_.end() ; ++i) {
        pools.insert(i->path());
    }
}

void PoolGroup::writable(eckit::StringSet& pools) const
{
    for (PoolVec::const_iterator i = pools_.begin(); i != pools_.end() ; ++i) {
        if(i->writable()) {
            pools.insert(i->path());
        }
    }
}

void PoolGroup::visitable(eckit::StringSet& pools) const
{
    for (PoolVec::const_iterator i = pools_.begin(); i != pools_.end() ; ++i) {
        if(i->visit()) {
            pools.insert(i->path());
        }
    }
}

bool PoolGroup::match(const std::string& s) const {
    return re_.match(s);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
