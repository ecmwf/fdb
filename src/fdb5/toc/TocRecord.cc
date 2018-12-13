/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sys/types.h>
#include <pwd.h>

#include "mars_server_version.h"

#include <iomanip>

#include "TocRecord.h"

#include "eckit/log/TimeStamp.h"
#include "eckit/runtime/Main.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

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

        std::string host = eckit::Main::hostname();
        host = host.substr(0, host.find(".")); // guaranteed to be less than 64 chars -- seee RFC 1035
        hostname_ = host;
    }
}

//----------------------------------------------------------------------------------------------------------------------

TocRecord::TocRecord(unsigned char tag):
    header_(tag) {
}

void TocRecord::dump(std::ostream& out, bool simple) const {

    switch (header_.tag_) {
        case TocRecord::TOC_INIT:
            out << "TOC_INIT ";
            break;

        case TocRecord::TOC_INDEX:
            out << "TOC_INDEX";
            break;

        case TocRecord::TOC_CLEAR:
            out << "TOC_CLEAR";
            break;

        case TocRecord::TOC_SUB_TOC:
            out << "TOC_SUB_TOC";
            break;

        default:
            out << "TOC_???? ";
            break;
    }

    std::ostringstream oss;
    oss << (header_.timestamp_.tv_usec / 1000000.0);

    out << " "
        << eckit::TimeStamp(header_.timestamp_.tv_sec)
        << "."
        << std::setw(6)
        << std::left
        << std::setfill('0')
        << oss.str().substr(2)
        << std::setfill(' ')
        << ", version:"
        << header_.version_
        << ", fdb: "
        << header_.fdbVersion_
        << ", uid: "
        << std::setw(4);

    struct passwd * p = getpwuid(header_.uid_);

    if (p) {
        out  << p->pw_name;
    } else {
        out << header_.uid_;
    }

    out << ", pid "
        << std::setw(5)
        << header_.pid_
        << ", host: "
        << header_.hostname_;

    if(!simple) { out << std::endl; }
}

void TocRecord::print(std::ostream & out) const {
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
    return 2;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
