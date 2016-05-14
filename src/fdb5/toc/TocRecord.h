/*
 * (C) Copyright 1996-2013 ECMWF.
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

#include <unistd.h>
#include <time.h>
#include <sys/time.h>

#include "eckit/types/FixedString.h"
#include "eckit/filesystem/PathName.h"

namespace fdb5 {

//-----------------------------------------------------------------------------

struct TocRecord {

    enum {
        TOC_NULL = 0,
        TOC_INIT = 't',
        TOC_INDEX = 'i',
        TOC_CLEAR = 'c'
    };

    static const size_t maxPayloadSize = 1024 * 1024;

    TocRecord( unsigned char tag = TOC_NULL);

    static unsigned int currentVersion();

    struct Header {
        unsigned char          tag_;           ///<  (1) tag identifying the TocRecord type
        unsigned char          spare_[3];      /// padding
        unsigned int           version_;       ///<  (1) tag version of the TocRecord type
        unsigned int           fdbVersion_;    ///<  (4) version of FDB writing this entry
        timeval                timestamp_;     ///<  (16) date & time of entry (in Unix seconds)
        pid_t                  pid_;           ///<  (4) process PID
        uid_t                  uid_;           ///<  (4) user ID
        eckit::FixedString<64> hostname_;      ///<  (64) hostname adding entry
        size_t                 size_;          ///< Record size

        Header(unsigned char tag);
    };

    Header                 header_;
    unsigned char          payload_[maxPayloadSize];

    static const size_t headerSize = sizeof(Header);

    void dump(std::ostream &out) const;

    void print(std::ostream &out) const;

    friend std::ostream &operator<<(std::ostream &s, const TocRecord &r) {
        r.print(s);
        return s;
    }

};

//-----------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_TocRecord_H
