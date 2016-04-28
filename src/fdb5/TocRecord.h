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

#define TOC_INIT	't'
#define TOC_INDEX	'i'
#define TOC_CLEAR	'c'
#define TOC_WIPE	'w'

struct TocRecord {

    typedef eckit::FixedString< 32 > MetaData;

	struct Head {

		unsigned char   tag_;					///<  (1) tag identifying the TocRecord type
		unsigned char   tagVersion_;			///<  (1) tag version of the TocRecord type

		unsigned char   unused_[2];				///   (2) reserved for future use

		unsigned int    fdbVersion_;			///<  (4) version of FDB writing this entry

		timeval         timestamp_;				///<  (16) date & time of entry (in Unix seconds)

		pid_t			pid_;					///<  (4) process PID
		uid_t			uid_;					///<  (4) user ID

		eckit::FixedString<64> hostname_;		///<  (64) hostname adding entry

		unsigned char   unused2_[32];			///<  (32) reserved for future use

	} head_;

	MetaData metadata_;

    static const size_t size = 4096;
    static const size_t payload_size = TocRecord::size - sizeof(Head) - sizeof(MetaData) - 2;

	eckit::FixedString< payload_size > payload_;

	unsigned char marker_[2];				    ///<  (2) termination marker

public: // methods

    /// default constructor without initialization
    TocRecord();

    /// constructor with initialization
    TocRecord( unsigned char tag );

    unsigned char version() const;

    bool isComplete() const;

    eckit::PathName path(const eckit::PathName& tocdir) const;

    MetaData metadata() const;

    friend std::ostream& operator<<(std::ostream& s,const TocRecord& x) { x.print(s); return s; }

    void print( std::ostream& out ) const;

    static unsigned char currentTagVersion();
};

//-----------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb_TocRecord_H
