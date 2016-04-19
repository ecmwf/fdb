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

#ifndef fdb5_TocDBWriter_H
#define fdb5_TocDBWriter_H

#include "fdb5/Index.h"
#include "fdb5/TocRecord.h"

#include "fdb5/TocDB.h"

namespace fdb5 {

class Key;
class TocIndex;

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocDBWriter : public TocDB {

public: // methods

    TocDBWriter(const Key& key);

    virtual ~TocDBWriter();

    /// Used for adopting & indexing external data to the TOC dir
    void index(const Key& key, const eckit::PathName& path, eckit::Offset offset, eckit::Length length);

protected: // methods

    virtual bool selectIndex(const Key& key);

    virtual bool open();

    virtual void archive(const Key& key, const void* data, eckit::Length length);

    virtual void flush();

    virtual void close();

    virtual void print( std::ostream& out ) const;

private: // types

    typedef std::map< std::string, eckit::DataHandle* >  HandleStore;

    typedef std::map< Key, TocIndex* > TocIndexStore;
    typedef std::map< Key, std::string > PathStore;

private: // methods

    /// Opens an Index with the associated path
    virtual Index* openIndex(const Key& key, const eckit::PathName& path ) const;

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

    eckit::PathName generateIndexPath(const Key& key) const;

    eckit::PathName generateDataPath(const Key& key) const;

private: // members

    HandleStore     handles_;    ///< stores the DataHandles being used by the Session

    TocIndexStore tocEntries_;
    PathStore     dataPaths_;

    long				blockSize_;
    std::vector<char>   padding_;

    std::set<Key> seen_;

    bool aio_;

    Index* current_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
