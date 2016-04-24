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

#ifndef fdb5_ToList_H
#define fdb5_ToList_H


#include "fdb5/TocHandler.h"

namespace fdb5 {


//-----------------------------------------------------------------------------

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

} // namespace fdb5

#endif
