/*
 * (C) Copyright 1996- ECMWF.
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

#include "fdb5/database/Index.h"
#include "fdb5/toc/TocRecord.h"

#include "fdb5/toc/TocDB.h"

namespace fdb5 {

class Key;
class TocAddIndex;

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocDBWriter : public TocDB {

public: // methods

    TocDBWriter(const Key &key, const eckit::Configuration& config);
    TocDBWriter(const eckit::PathName& directory, const eckit::Configuration& config);

    virtual ~TocDBWriter();

    /// Used for adopting & indexing external data to the TOC dir
    void index(const Key &key, const eckit::PathName &path, eckit::Offset offset, eckit::Length length);

protected: // methods

    virtual bool selectIndex(const Key &key);
    virtual void deselectIndex();

    virtual bool open();
    virtual void flush();
    virtual void close();

    virtual void archive(const Key &key, const void *data, eckit::Length length);


    virtual void print( std::ostream &out ) const;

private: // methods


    eckit::DataHandle *getCachedHandle( const eckit::PathName &path ) const;
    eckit::DataHandle *createFileHandle(const eckit::PathName &path);
    eckit::DataHandle *createAsyncHandle(const eckit::PathName &path);

    void closeIndexes();
    void closeDataHandles();
    void flushIndexes();
    void flushDataHandles();
    void compactSubTocIndexes();

    eckit::DataHandle &getDataHandle( const eckit::PathName & );
    eckit::PathName getDataPath(const Key &key);
    eckit::PathName generateIndexPath(const Key &key) const;
    eckit::PathName generateDataPath(const Key &key) const;

private: // types

    typedef std::map< std::string, eckit::DataHandle * >  HandleStore;
    typedef std::map< Key, Index> IndexStore;
    typedef std::map< Key, std::string > PathStore;

private: // members

    HandleStore handles_;    ///< stores the DataHandles being used by the Session

    // If we have multiple flush statements, then the indexes get repeatedly reset. Build and maintain
    // a full copy of the indexes associated with the process as well, for use when masking out
    // subtocs. See compactSubTocIndexes.
    IndexStore  indexes_;
    IndexStore  fullIndexes_;

    PathStore   dataPaths_;

    Index current_;
    Index currentFull_;
    Key currentIndexKey_;

    bool dirty_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
