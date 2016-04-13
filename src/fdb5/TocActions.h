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

#ifndef fdb5_TocActions_H
#define fdb5_TocActions_H

#include <unistd.h>
#include <limits.h>
#include <stdlib.h>

#include "eckit/eckit.h"
#include "eckit/types/DateTime.h"

#include "eckit/io/Length.h"
#include "eckit/types/FixedString.h"
#include "eckit/utils/MD5.h"
#include "eckit/memory/Padded.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/memory/NonCopyable.h"

#include "fdb5/TocRecord.h"
#include "fdb5/TocHandler.h"
#include "fdb5/Index.h"

namespace fdb5 {

class TocSchema;

//-----------------------------------------------------------------------------

/// Initialises an FDB directory with an empty Toc
/// On existing directories has no effect.
class TocInitialiser : public TocHandler {

public: // methods

	TocInitialiser( const eckit::PathName& dir );

};

//-----------------------------------------------------------------------------

class TocIndex : public TocHandler {

public: // methods

    TocIndex(const eckit::PathName& dir, const eckit::PathName& idx, const fdb5::Key& key);

	~TocIndex();

	eckit::PathName index() const;

private: // members

	eckit::PathName index_;
	TocRecord::MetaData tocMD_;

};

//-----------------------------------------------------------------------------

/// Caches a list of the contents of the Toc

class TocReverseIndexes : public TocHandler {

public: // methods

    TocReverseIndexes( const eckit::PathName& dir );

    std::vector< eckit::PathName > indexes(const Key& key) const;

private:

    void init(); ///< May be called multiple times

    typedef std::map< TocRecord::MetaData, TocPaths > TocMap;

	TocVec toc_;

    mutable TocMap cacheIndexes_;

    bool inited_;
};

//-----------------------------------------------------------------------------

/// Caches a list of the contents of the Toc

class TocList : public TocHandler {

public: // methods

	TocList( const eckit::PathName& dir );

	const std::vector<TocRecord>& list();

	void list( std::ostream& ) const;

private: // members

	std::vector<TocRecord> toc_;

};

//-----------------------------------------------------------------------------

class TocPrint : public TocHandler {

public: // methods

	TocPrint( const eckit::PathName& dir );

	void print( std::ostream& os );
};

//-----------------------------------------------------------------------------

} // namespace fdb5

#endif
