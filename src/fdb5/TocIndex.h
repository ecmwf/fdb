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

#ifndef fdb5_TocIndex_H
#define fdb5_TocIndex_H

#include "fdb5/TocHandler.h"

namespace fdb5 {

class Key;

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

} // namespace fdb5

#endif
