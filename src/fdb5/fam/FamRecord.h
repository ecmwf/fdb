/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   FamRecord.h
/// @author Metin Cakircali
/// @date   Jul 2024

#pragma once

#include "eckit/types/FixedString.h"
#include "fdb5/fam/FamRecordVersion.h"

#include <sys/time.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <ostream>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

struct FamRecord {
    // types

    using size_type    = std::size_t;
    using value_type   = unsigned char;
    using version_type = FamRecordVersion::value_type;
    using host_type    = eckit::FixedString<64>;

    enum class Tag : value_type { TOC_NULL = '\0', TOC_INIT = 't', TOC_INDEX = 'i', TOC_CLEAR = 'c', TOC_SUBTOC = 's' };

    struct Header {
        Tag          tag_ {Tag::TOC_NULL};  //  1-byte : record type
        version_type version_;              //  1-byte : record version
        value_type   spare_[2] {};          //  2-byte : padding
        unsigned int fdbVersion_;           //  4-byte : FDB version
        timeval      timestamp_ {};         // 16-byte : date and time (consider UUID?)
        gid_t        gid_;                  //  4-byte : group ID
        uid_t        uid_;                  //  4-byte : user ID
        host_type    hostname_;             // 64-byte : hostname
        size_type    size_ {0};             //  8-byte : payload size

        explicit Header(version_type version, Tag tag);
    };

    static constexpr size_type capacity   = 1024 * 1024;
    static constexpr size_type headerSize = sizeof(Header);

    // methods

    FamRecord(version_type version, Tag tag = Tag::TOC_NULL);

    void dump(std::ostream& out) const;

    // void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& out, const FamRecord& record);

    // members

    Header     header_;
    value_type payload_[capacity] {};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
