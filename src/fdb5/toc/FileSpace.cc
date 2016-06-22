/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/FileSpace.h"

#include "eckit/filesystem/FileSpaceStrategies.h"

#include "fdb5/toc/TocHandler.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FileSpace::FileSpace(const std::string& name,
                     const std::string& re,
                     const std::string& handler,
                     const std::vector<Root>& roots) :
    name_(name),
    handler_(handler),
    re_(re),
    roots_(roots) {
}

eckit::PathName FileSpace::filesystem(const Key& key) const
{
    eckit::PathName result;

    // check that the database isn't present already
    // if it is, then return that path

    if(existsDB(key, result)) return result;

    // no existing DB found so use the handler to select the strategy

    /// TODO: make a proper handler

    return eckit::FileSpaceStrategies::selectFileSystem(writable(), handler_);
}

std::vector<eckit::PathName> FileSpace::writable() const
{
    std::vector<eckit::PathName> result;
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end() ; ++i) {
        if(i->writable()) {
            result.push_back(i->path());
        }
    }
    return result;
}

void FileSpace::writable(eckit::StringSet& roots) const
{
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end() ; ++i) {
        if(i->writable()) {
            roots.insert(i->path());
        }
    }
}

void FileSpace::visitable(eckit::StringSet& roots) const
{
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end() ; ++i) {
        if(i->visit()) {
            roots.insert(i->path());
        }
    }
}

bool FileSpace::match(const std::string& s) const {
    return re_.match(s);
}

bool FileSpace::existsDB(const Key& key, eckit::PathName& path) const
{
    std::string subdir = key.valuesToString();
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end() ; ++i) {
        eckit::PathName dbpath = i->path() / subdir;
        fdb5::TocHandler handler(dbpath);
        if(handler.exists()) {
            path = dbpath;
            return true;
        }
    }
    return false;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
