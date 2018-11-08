/*
 * (C) Copyright 1996- ECMWF.
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

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/memory/ScopedPtr.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/io/LustreFileHandle.h"
#include "fdb5/toc/TocRecord.h"

#include <map>

namespace eckit {
class Configuration;
}

namespace fdb5 {

class Key;
class Index;

//-----------------------------------------------------------------------------

class TocHandler : private eckit::NonCopyable {

public: // typedefs

    typedef std::vector<TocRecord> TocVec;
    typedef std::vector< eckit::PathName > TocPaths;

public: // methods

    TocHandler( const eckit::PathName &dir, const Config& config=Config());

    /// For initialising sub tocs or diagnostic interrogation. Bool just for identification.
    TocHandler(const eckit::PathName& path, bool);

    ~TocHandler();

    bool exists() const;
    void checkUID() const;

    void writeInitRecord(const Key &tocKey);
    void writeClearRecord(const Index &);
    void writeSubTocRecord(const TocHandler& subToc);
    void writeIndexRecord(const Index &);

    bool useSubToc() const;
    bool anythingWrittenToSubToc() const;

    std::vector<Index> loadIndexes(bool sorted=false) const;

    Key databaseKey();
    size_t numberOfRecords() const;

    const eckit::PathName& directory() const;
    const eckit::PathName& tocPath() const;
    const eckit::PathName& schemaPath() const;

    void dump(std::ostream& out, bool simple = false, bool walkSubTocs = true) const;
    void dumpIndexFile(std::ostream& out, const eckit::PathName& indexFile) const;
    std::string dbOwner() const;

    DbStats stats() const;

protected: // methods

    size_t tocFilesSize() const;

    std::vector<eckit::PathName> subTocPaths() const;

protected: // members

    const eckit::PathName directory_;
    mutable long dbUID_;
    long userUID_;

    long dbUID() const;

protected: // methods

    static bool stripeLustre();

    static LustreStripe stripeIndexLustreSettings();

    static LustreStripe stripeDataLustreSettings();

    // Build the record, and return the payload size

    static size_t buildIndexRecord(TocRecord& r, const Index& index);
    size_t buildSubTocMaskRecord(TocRecord& r);

    // Given the payload size, returns the record size

    static size_t roundRecord(TocRecord &r, size_t payloadSize);

    void appendBlock(const void* data, size_t size);

private: // methods

    friend class TocHandlerCloser;

    void openForAppend();

    void openForRead() const;

    void close() const;

    /// Populate the masked sub toc list, starting from the _current_position_ in the
    /// file (opened for read). It resets back to the same place when done. This is
    /// to allow searching only from the first subtoc.
    void populateMaskedSubTocsList() const;

    void append(TocRecord &r, size_t payloadSize);

    // hideSubTocEntries=true returns entries as though only one toc existed (i.e. to hide
    // the mechanism of subtocs).
    bool readNext(TocRecord &r, bool walkSubTocs = true, bool hideSubTocEntries = true) const;

    bool readNextInternal(TocRecord &r) const;

    std::string userName(long) const;

    static size_t recordRoundSize();

private: // members

    eckit::PathName tocPath_;
    eckit::PathName schemaPath_;
    Config dbConfig_;

    bool useSubToc_;
    bool isSubToc_;

    mutable int fd_;      ///< file descriptor, if zero file is not yet open.

    /// The sub toc is initialised in the read or write pathways for maintaining state.
    mutable eckit::ScopedPtr<TocHandler> subTocRead_;
    mutable eckit::ScopedPtr<TocHandler> subTocWrite_;
    mutable size_t count_;

    mutable std::vector<eckit::PathName> maskedSubTocs_;

    mutable bool enumeratedMaskedSubTocs_;
    mutable bool writeMode_;
};


//-----------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb_TocHandler_H
