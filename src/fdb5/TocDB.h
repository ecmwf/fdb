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

#ifndef fdb5_TocDB_H
#define fdb5_TocDB_H

#include "fdb5/DB.h"
#include "fdb5/TocSchema.h"
#include "fdb5/Index.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocDB : public DB {

public: // methods

    TocDB(const Key& dbKey);

    virtual ~TocDB();

protected: // types

    typedef std::map< std::string, Index* > IndexStore;

protected: // methods

    virtual bool match(const Key& key) const;

    virtual const Schema& schema() const;

    virtual void archive(const Key& key, const void* data, eckit::Length length);

    virtual void flush();

    virtual eckit::DataHandle* retrieve(const MarsTask& task, const Key& key) const;

    virtual Index* openIndex( const eckit::PathName& path ) const = 0;

    /// Get the Index on the given path
    /// If necessary it creates it and inserts in the cache
    Index& getIndex( const eckit::PathName& path ) const;

    /// @param path PathName to the Index
    /// @returns the cached eckit::DataHandle or NULL if not found
    Index* getCachedIndex( const eckit::PathName& path ) const;

    /// Closes all indexes in the cache
    void closeIndexes();

protected: // members

    TocSchema schema_;

    mutable IndexStore indexes_;    ///< stores the indexes being used by the Session

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
