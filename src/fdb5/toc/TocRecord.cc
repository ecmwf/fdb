/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <pwd.h>
#include <sys/types.h>

#include "fdb5/fdb5_version.h"

#include <iomanip>

#include "TocRecord.h"

#include "eckit/log/TimeStamp.h"
#include "eckit/runtime/Main.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocRecord::Header::Header(unsigned int serialisationVersion, unsigned char tag) :
    tag_(tag),
    spare_{},
    serialisationVersion_(tag == TOC_NULL ? 0 : serialisationVersion),
    fdbVersion_(tag == TOC_NULL ? 0 : ::fdb5_version_int()),
    timestamp_{},
    pid_(tag == TOC_NULL ? 0 : ::getpid()),
    uid_(tag == TOC_NULL ? 0 : ::getuid()),
    hostname_{},
    size_(0) {
    if (tag_ != TOC_NULL) {
        SYSCALL(::gettimeofday(&timestamp_, 0));

        std::string host = eckit::Main::hostname();
        // host is guaranteed to be less than 64 chars -- seee RFC 1035
        host      = host.substr(0, host.find("."));
        hostname_ = host;
    }
}

//----------------------------------------------------------------------------------------------------------------------

TocRecord::TocRecord(unsigned int serialisationVersion, unsigned char tag) : header_(serialisationVersion, tag) {}

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

    out << " " << eckit::TimeStamp(header_.timestamp_.tv_sec) << "." << std::setw(6) << std::left << std::setfill('0')
        << oss.str().substr(2) << std::setfill(' ') << ", version:" << header_.serialisationVersion_
        << ", fdb: " << header_.fdbVersion_ << ", uid: " << std::setw(4);

    struct passwd* p = getpwuid(header_.uid_);

    if (p) {
        out << p->pw_name;
    }
    else {
        out << header_.uid_;
    }

    out << ", pid " << std::setw(5) << header_.pid_ << ", host: " << header_.hostname_;

    if (!simple) {
        out << std::endl;
    }
}

void TocRecord::print(std::ostream& out) const {
    out << "TocRecord["
        << "tag=" << header_.tag_ << ","
        << "tocVersion=" << header_.serialisationVersion_ << ","
        << "fdbVersion=" << header_.fdbVersion_ << ","
        << "timestamp=" << header_.timestamp_.tv_sec << "." << header_.timestamp_.tv_usec << ","
        << "pid=" << header_.pid_ << ","
        << "uid=" << header_.uid_ << ","
        << "hostname=" << header_.hostname_ << "]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
