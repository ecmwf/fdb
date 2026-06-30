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

#include "fdb5/LibFdb5.h"
#include "fdb5/toc/Root.h"

using eckit::Log;
using eckit::Stat;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Root::Root(const std::string& path, const std::string& filespace, bool list, bool retrieve, bool archive, bool wipe) :
    path_(path), filespace_(filespace), checked_(false), exists_(false), controlIdentifiers_() {
    if (!list) {
        controlIdentifiers_ |= ControlIdentifier::List;
    }
    if (!retrieve) {
        controlIdentifiers_ |= ControlIdentifier::Retrieve;
    }
    if (!archive) {
        controlIdentifiers_ |= ControlIdentifier::Archive;
    }
    if (!wipe) {
        controlIdentifiers_ |= ControlIdentifier::Wipe;
    }
}

bool Root::exists() const {
    if (!checked_) {
        errno = 0;
        Stat::Struct info;
        int err = Stat::stat(path_.asString().c_str(), &info);
        if (not err) {
            exists_ = S_ISDIR(info.st_mode);
        }
        else {
            Log::warning() << "FDB root " << path_ << " " << Log::syserr << std::endl;
            exists_ = false;
        }
        LOG_DEBUG_LIB(LibFdb5) << "Root " << *this << (exists_ ? " exists" : " does NOT exists") << std::endl;
        checked_ = true;
    }

    return exists_;
}

const eckit::PathName& Root::path() const {
    return path_;
}

const std::string& Root::filespace() const {
    return filespace_;
}

void Root::print(std::ostream& out) const {

    out << "Root("
        << "path=" << path_ << ",controlIdentifiers=" << controlIdentifiers_ << ")";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
