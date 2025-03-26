/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Tiago Quintino
/// @date Dec 2014

#ifndef fdb5_TocRecord_H
#define fdb5_TocRecord_H

#include <sys/time.h>
#include <unistd.h>
#include <ctime>

#include "eckit/filesystem/PathName.h"
#include "eckit/types/FixedString.h"
#include "eckit/utils/Literals.h"

using namespace eckit::literals;

namespace fdb5 {

//-----------------------------------------------------------------------------

struct TocRecord {

    /// We use TOC_CLEAR for two purposes.
    ///
    ///  i) Indicating that an index should no longer be visited
    /// ii) Indicating that a sub-toc should no longer be visited
    ///
    /// If an older version of the software is used that does not recognise sub toc masking,
    /// then this use of TOC_CLEAR will have no impact. Therefore this is backward compatible
    /// and does not require bumping the format version (making roll-out more straightforward).

    enum {
        TOC_NULL    = 0,
        TOC_INIT    = 't',
        TOC_INDEX   = 'i',
        TOC_CLEAR   = 'c',
        TOC_SUB_TOC = 's'
    };

    static const size_t maxPayloadSize = 1_MiB;

    TocRecord(unsigned int serialisationVersion, unsigned char tag = TOC_NULL);

    struct Header {
        unsigned char tag_;                  ///<  (1)  tag identifying the TocRecord type
        unsigned char spare_[3];             ///<  (3)  padding
        unsigned int serialisationVersion_;  ///<  (4)  serialisation version of the TocRecord
        unsigned int fdbVersion_;            ///<  (4)  version of FDB writing this entry
        timeval timestamp_;                  ///<  (16) date & time of entry (in Unix seconds)
        pid_t pid_;                          ///<  (4)  process PID
        uid_t uid_;                          ///<  (4)  user ID
        eckit::FixedString<64> hostname_;    ///<  (64) hostname adding entry
        size_t size_;                        ///<  (8)  Record size

        Header(unsigned int serialisationVersion, unsigned char tag);
    };

    Header header_;
    unsigned char payload_[maxPayloadSize];

    static const size_t headerSize = sizeof(Header);

    void dump(std::ostream& out, bool simple = false) const;

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& s, const TocRecord& r) {
        r.print(s);
        return s;
    }
};

//-----------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb5_TocRecord_H
