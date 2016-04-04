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

#include "fdb5/DB.h"
#include "fdb5/TocSchema.h"

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocDB : public DB {

public: // methods

    TocDB(const Key& key);

    virtual ~TocDB();

    virtual bool match(const Key& key) const;

    virtual const Schema& schema() const;

    virtual void archive(const Key& key, const void* data, eckit::Length length);

    virtual void flush();

    virtual eckit::DataHandle* retrieve(const FdbTask& task, const Key& key) const;

protected: // methods

    virtual void print( std::ostream& out ) const;

private: // members

    TocSchema schema_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif
