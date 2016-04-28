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

#ifndef fdb5_TocHandler_H
#define fdb5_TocHandler_H

#include "eckit/io/Length.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/TocRecord.h"

namespace fdb5 {

class Key;

//-----------------------------------------------------------------------------

class TocHandler : private eckit::NonCopyable {

public: // typedefs

	typedef std::vector<TocRecord> TocVec;
	typedef std::vector< eckit::PathName > TocPaths;

public: // methods

	/// contructor for Toc's that already exist
	TocHandler( const eckit::PathName& dir );

	~TocHandler();

    eckit::PathName filePath() const { return filePath_; }
	eckit::PathName dirPath() const { return dir_; }

    bool isOpen() const { return fd_ >= 0; }
    bool exists() const;

    friend std::ostream& operator<<(std::ostream& s,const TocHandler& x) { x.print(s); return s; }

protected: // methods

	void openForAppend();
    void openForRead();

	void append( const TocRecord& r );

    size_t readNext( TocRecord& r );

	/// Closes the file descriptor if it is still open. Always safe to call.
	void close();

	void printRecord( const TocRecord& r, std::ostream& os );

    TocRecord makeRecordTocInit(const Key& key) const;
    TocRecord makeRecordIdxInsert(const eckit::PathName& path, const Key& key) const;
    TocRecord makeRecordIdxRemove(const eckit::PathName& path) const;
	TocRecord makeRecordTocWipe() const;

    void print( std::ostream& out ) const;

protected: // members

    eckit::PathName dir_;
    eckit::PathName filePath_;

	int fd_;      ///< file descriptor, if zero file is not yet open.

	bool read_;
};

//-----------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb_TocHandler_H
