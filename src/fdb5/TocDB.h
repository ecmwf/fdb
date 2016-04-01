/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocDB.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_TocDB_H
#define fdb_TocDB_H

#include <vector>
#include <string>
#include <iosfwd>

#include "fdb5/DB.h"

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

class TocDB : public DB {

public: // methods

	TocDB();

    virtual ~TocDB();

    virtual std::vector<std::string> schema() const;

    virtual eckit::DataHandle* retrieve(const FdbTask& task, const marskit::MarsRequest& field) const;

protected: // methods

    virtual void print( std::ostream& out ) const;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif
