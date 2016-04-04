/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocDBWriter.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_TocDBWriter_H
#define fdb_TocDBWriter_H

#include "fdb5/Index.h"
#include "fdb5/TocRecord.h"

#include "fdb5/TocDB.h"

namespace fdb5 {

class TocIndex;

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocDBWriter : public TocDB {

public: // methods

    TocDBWriter(const Key& key);

    virtual ~TocDBWriter();

    virtual void archive(const Key& userkey, const void* data, eckit::Length length);

    virtual void flush();

protected: // methods

    virtual void print( std::ostream& out ) const;

private: // types

    typedef std::map< std::string, Index* > IndexStore;
    typedef std::map< std::string, eckit::DataHandle* >  HandleStore;

    typedef std::map< std::string, TocIndex* > TocIndexStore;
    typedef std::map< std::string, std::string > PathStore;

private: // methods

    /// Get the Index on the given path
    /// If necessary it creates it and inserts in the cache
    Index& getIndex( const eckit::PathName& path );

    /// Opens an Index with the associated path
    Index* openIndex( const eckit::PathName& path ) const;

    /// @param path PathName to the Index
    /// @returns the cached eckit::DataHandle or NULL if not found
    Index* getCachedIndex( const eckit::PathName& path ) const;

    /// Closes all indexes in the cache
    void closeIndexes();

    /// @param path PathName to the handle
    /// @returns the cached eckit::DataHandle or NULL if not found
    eckit::DataHandle* getCachedHandle( const eckit::PathName& path ) const;

    void closeDataHandles();

    eckit::DataHandle* createFileHandle(const eckit::PathName& path);
    eckit::DataHandle* createAsyncHandle(const eckit::PathName& path);
    eckit::DataHandle* createPartHandle(const eckit::PathName& path, eckit::Offset offset, eckit::Length length);

    TocIndex& getTocIndex(const Key& key);

    void flushIndexes();
    void flushDataHandles();

    void closeTocEntries();

    eckit::DataHandle& getDataHandle( const eckit::PathName& );

    eckit::PathName getDataPath(const Key& key);

private: // members

    IndexStore      indexes_;    ///< stores the indexes being used by the Session
    HandleStore     handles_;    ///< stores the DataHandles being used by the Session

    TocIndexStore tocEntries_;
    PathStore     dataPaths;

    std::string indexType_; ///< type of index to build

    long				blockSize_;
    std::vector<char>   padding_;

    bool            aio_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
