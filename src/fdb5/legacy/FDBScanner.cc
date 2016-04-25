/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/types/Types.h"
#include "eckit/config/Resource.h"
#include "eckit/thread/ThreadPool.h"

#include "fdb5/Error.h"

#include "fdb5/legacy/FDBIndexScanner.h"
#include "fdb5/legacy/FDBScanner.h"

using namespace eckit;

namespace fdb5 {
namespace legacy {

//----------------------------------------------------------------------------------------------------------------------

FDBScanner::FDBScanner(const eckit::PathName& path) :
    path_(path)
{
    if(!path_.isDir()) {
        std::ostringstream oss;
        oss << "Path to FDB isn't a directory: " << path_ << std::endl;
        throw fdb5::Error(Here(),oss.str());
    }
}

FDBScanner::~FDBScanner()
{
}

void FDBScanner::execute()
{
//    std::string root = eckit::Resource<std::string>( "fdbRoot;$FDB_ROOT","/tmp/fdb" );
//    std::vector<std::string> dirs = Translator<std::string, StringList >()(Resource<std::string>("-dirs;fdbDirs", "/:*"));
         // e.g. "/:od:enfo:g:0001:20121003::"

    Log::info() << "Scanning FDB db " << path_ << std::endl;

    std::string pattern = eckit::Resource<std::string>("fdbPattern", "/:*.");
    //  e.g. "/*[pcf][fc]:0000:*."

    eckit::PathName path(path_);
    path /= pattern;

    std::vector<PathName> indexes;
    eckit::PathName::match(path, indexes, true);

    for(std::vector<PathName>::const_iterator j = indexes.begin(); j != indexes.end(); ++j) {
        pool().push(new FDBIndexScanner(*j));
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5
