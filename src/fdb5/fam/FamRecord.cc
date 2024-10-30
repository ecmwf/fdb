/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence version_type 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/fam/FamRecord.h"

#include "eckit/memory/Zero.h"
#include "fdb5/fdb5_version.h"

#include <unistd.h>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
// TAG

std::ostream& operator<<(std::ostream& out, const FamRecord::Tag& tag) {
    switch (tag) {
        case FamRecord::Tag::TOC_NULL:   out << "TOC_NULL"; break;
        case FamRecord::Tag::TOC_INIT:   out << "TOC_INIT"; break;
        case FamRecord::Tag::TOC_INDEX:  out << "TOC_INDEX"; break;
        case FamRecord::Tag::TOC_CLEAR:  out << "TOC_CLEAR"; break;
        case FamRecord::Tag::TOC_SUBTOC: out << "TOC_SUBTOC"; break;
        default:                         out << "TOC_??????"; break;
    }
    return out;
}

//----------------------------------------------------------------------------------------------------------------------
// HEADER

FamRecord::Header::Header(const version_type version, const Tag tag): tag_ {tag} {
    if (tag_ != Tag::TOC_NULL) {
        eckit::zero(*this);

        tag_        = tag;
        version_    = version;
        fdbVersion_ = ::fdb5_version_int();

        SYSCALL(::gettimeofday(&timestamp_, nullptr));

        gid_ = ::getgid();
        uid_ = ::getuid();
        // pid_ = ::getpid();

        SYSCALL(::gethostname(hostname_.data(), sizeof(host_type) - 1));
    }
}

//----------------------------------------------------------------------------------------------------------------------
// RECORD

FamRecord::FamRecord(const version_type version, const Tag tag): header_(version, tag) { }

void FamRecord::dump(std::ostream& out) const {
    out << "TocRecord[tag=" << header_.tag_ << ",version=" << header_.version_ << ",fdb=" << header_.fdbVersion_
        << ",timestamp=" << header_.timestamp_.tv_sec << "." << header_.timestamp_.tv_usec << ",gid=" << header_.gid_
        << ",uid=" << header_.uid_ << ",hostname=" << header_.hostname_ << ",size: " << header_.size_ << ']';
}

std::ostream& operator<<(std::ostream& out, const FamRecord& record) {
    record.dump(out);
    return out;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
