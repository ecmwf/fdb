/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"
#include "eckit/os/Stat.h"

#include "fdb5/toc/Root.h"
#include "fdb5/LibFdb5.h"

using eckit::Log;
using eckit::Stat;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Root::Root(const std::string &path, const std::string& filespace,
           bool list, bool retrieve, bool archive, bool wipe):
    path_(path),
    filespace_(filespace),
    permission_(list, retrieve, archive, wipe)
{
    errno = 0;
    Stat::Struct info;
    int err = Stat::stat(path_.asString().c_str(),&info);
    if(not err) {
        exists_ = S_ISDIR(info.st_mode);
    }
    else {
        Log::warning() << "FDB root " << path_ << " " << Log::syserr << std::endl;
        exists_ = false;
    }
    Log::debug<LibFdb5>() << "Root " << *this << (exists_ ? " exists" : " does NOT exists") << std::endl;
}

const eckit::PathName& Root::path() const {
    return path_;
}

const std::string& Root::filespace() const
{
    return filespace_;
}

void Root::print( std::ostream &out ) const  {

    out << "Root("
        << "path=" << path_
        << ",permission=" << permission_
        <<")";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
