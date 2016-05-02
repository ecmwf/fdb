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

#include "fdb5/toc/TocRecord.h"

namespace fdb5 {

class Key;
class Index;

//-----------------------------------------------------------------------------

class TocHandler : private eckit::NonCopyable {

public: // typedefs

    typedef std::vector<TocRecord> TocVec;
    typedef std::vector< eckit::PathName > TocPaths;

public: // methods

    TocHandler( const eckit::PathName &dir );
    ~TocHandler();

    bool exists() const;

    void writeInitRecord(const Key &tocKey);
    void writeClearRecord(const Index &);
    void writeIndexRecord(const Index &);
    void writeWipeRecord();

    std::vector<Index *> loadIndexes();
    void freeIndexes(std::vector<Index *> &);

    Key databaseKey();

    static eckit::PathName directory(const Key &key);
    static std::vector<eckit::PathName> databases(const Key &key);

protected: // members

    const eckit::PathName directory_;

private: // methods

    friend class TocHandlerCloser;

    void openForAppend();
    void openForRead();
    void close();

    void append(TocRecord &r, size_t payloadSize);
    bool readNext(TocRecord &r);

    eckit::PathName filePath_;

    int fd_;      ///< file descriptor, if zero file is not yet open.

};

//-----------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb_TocHandler_H
