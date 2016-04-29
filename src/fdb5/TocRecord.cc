/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "mars_server_version.h"
#include "fdb5/TocRecord.h"

namespace fdb5 {



TocRecord::Header::Header(unsigned char tag):
    tag_(tag) {

    if (tag_ != TOC_NULL) {
        eckit::zero(*this);
        tag_        = tag;
        version_    = currentVersion();

        fdbVersion_ = ::mars_server_version_int();

        SYSCALL( ::gettimeofday( &timestamp_, 0 ) );

        pid_       = ::getpid();
        uid_       = ::getuid();

        //TODO: cache hostname
        SYSCALL( ::gethostname( hostname_.data(), hostname_.static_size() ) );
    }
}

//-----------------------------------------------------------------------------

TocRecord::TocRecord(unsigned char tag):
    header_(tag) {
}


void TocRecord::print(std::ostream &out) const {
    out << "TocRecord["
        << "tag=" << header_.tag_ << ","
        << "tagVersion=" << header_.version_ << ","
        << "fdbVersion=" << header_.fdbVersion_ << ","
        << "timestamp=" << header_.timestamp_.tv_sec << "." << header_.timestamp_.tv_usec << ","
        << "pid=" << header_.pid_ << ","
        << "uid=" << header_.uid_ << ","
        << "hostname=" << header_.hostname_ << "]";
}

unsigned int TocRecord::currentVersion() {
    return 1;
}

//-----------------------------------------------------------------------------

} // namespace fdb5
