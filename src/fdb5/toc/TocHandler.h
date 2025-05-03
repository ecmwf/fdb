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

#include <map>
#include <memory>
#include <utility>

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/log/Timer.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/toc/TocCommon.h"
#include "fdb5/toc/TocRecord.h"
#include "fdb5/toc/TocSerialisationVersion.h"


namespace eckit {
class Configuration;
class MemoryHandle;
}  // namespace eckit

namespace fdb5 {

class Key;
class Index;

//----------------------------------------------------------------------------------------------------------------------

extern const std::map<ControlIdentifier, const char*> controlfile_lookup;

//----------------------------------------------------------------------------------------------------------------------
class TocCopyWatcher : public eckit::TransferWatcher {

    void fromHandleOpened() override {
        timer_.start();
        time_ = timer_.elapsed();
    }

    void toHandleOpened() override {
        timer_.start();
        time_ = timer_.elapsed();
    }

    void watch(const void*, long len) override {
        double now = timer_.elapsed();
        tocCopyStats_.push_back(std::make_pair(now - time_, len));
        time_ = now;
    }

public:

    TocCopyWatcher() : idx_(0) {}

    size_t size() const { return tocCopyStats_.size(); }

    bool next(double& time, eckit::Length& len) {
        if (tocCopyStats_.size() > idx_) {
            time = tocCopyStats_[idx_].first;
            len  = tocCopyStats_[idx_].second;

            idx_++;
            return true;
        }
        return false;
    }

private:

    eckit::Timer timer_;
    double time_;

    size_t idx_;
    std::vector<std::pair<double, eckit::Length>> tocCopyStats_;
};

//-----------------------------------------------------------------------------

class TocHandler : public TocCommon, private eckit::NonCopyable {

public:  // typedefs

    typedef std::vector<TocRecord> TocVec;
    typedef std::vector<eckit::LocalPathName> TocPaths;

public:  // methods

    TocHandler(const eckit::PathName& dir, const Config& config);

    /// For initialising sub tocs or diagnostic interrogation.
    TocHandler(const eckit::PathName& path, const Key& parentKey, eckit::MemoryHandle* cachedToc = nullptr);

    ~TocHandler() override;

    bool exists() const;
    void checkUID() const override;

    void writeInitRecord(const Key& tocKey);
    void writeClearRecord(const Index&);
    void writeClearAllRecord();
    void writeSubTocRecord(const TocHandler& subToc);
    void writeIndexRecord(const Index&);
    void writeSubTocMaskRecord(const TocHandler& subToc);

    void reconsolidateIndexesAndTocs();

    bool useSubToc() const;
    bool anythingWrittenToSubToc() const;

    /// Return a list of existent indexes. If supplied, also supply a list of associated
    /// subTocs that were read to get these indexes
    std::vector<Index> loadIndexes(bool sorted = false, std::set<std::string>* subTocs = nullptr,
                                   std::vector<bool>* indexInSubtoc = nullptr,
                                   std::vector<Key>* remapKeys      = nullptr) const;

    Key databaseKey();
    size_t numberOfRecords() const;

    const eckit::LocalPathName& directory() const;
    const eckit::LocalPathName& tocPath() const;
    const eckit::LocalPathName& schemaPath() const;

    void dump(std::ostream& out, bool simple = false, bool walkSubTocs = true) const;
    void dumpIndexFile(std::ostream& out, const eckit::PathName& indexFile) const;
    std::string dbOwner() const;

    DbStats stats() const;

    void enumerateMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata, std::set<eckit::URI>& data) const;

    std::vector<eckit::PathName> subTocPaths() const;
    // Utilities for handling locks
    std::vector<eckit::PathName> lockfilePaths() const;

protected:  // methods

    size_t tocFilesSize() const;


    // Access and control of locks

    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const;

    bool enabled(const ControlIdentifier& controlIdentifier) const;

private:  // methods

    eckit::LocalPathName fullControlFilePath(const std::string& name) const;
    void createControlFile(const std::string& name) const;
    void removeControlFile(const std::string& name) const;

protected:  // members

    mutable Key parentKey_;  // Contains the key of the first TOC explored in subtoc chain

    uid_t dbUID() const override;

protected:  // methods

    // Handle location and remapping information if using a mounted TocCatalogue
    const eckit::LocalPathName& currentDirectory() const;
    const eckit::LocalPathName& currentTocPath() const;
    const Key& currentRemapKey() const;

    // Build the record, and return the payload size

    static size_t buildIndexRecord(TocRecord& r, const Index& index);
    static size_t buildClearRecord(TocRecord& r, const Index& index);
    size_t buildSubTocMaskRecord(TocRecord& r);
    static size_t buildSubTocMaskRecord(TocRecord& r, const eckit::PathName& path);

    // Given the payload size, returns the record size

    static std::pair<size_t, size_t> recordSizes(TocRecord& r, size_t payloadSize);

    void appendBlock(const void* data, size_t size);
    void appendBlock(TocRecord& r, size_t payloadSize);

    const TocSerialisationVersion& serialisationVersion() const;

private:  // methods

    friend class TocHandlerCloser;

    void openForAppend();

    void openForRead() const;

    void close() const;

    void appendRaw(const void* data, size_t size);
    void appendRound(TocRecord& r, size_t payloadSize);

    /// Populate the masked sub toc list, starting from the _current_position_ in the
    /// file (opened for read). It resets back to the same place when done. This is
    /// to allow searching only from the first subtoc.
    void allMaskableEntries(eckit::Offset startOffset, eckit::Offset endOffset,
                            std::set<std::pair<eckit::LocalPathName, eckit::Offset>>& maskedEntries) const;
    eckit::LocalPathName parseSubTocRecord(const TocRecord& r, bool readMasked) const;
    void populateMaskedEntriesList() const;
    void preloadSubTocs(bool readMasked) const;

    void append(TocRecord& r, size_t payloadSize);

    // Check if the Index record should be skipped
    bool ignoreIndex(const TocRecord& r, bool readMasked) const;

    // hideSubTocEntries=true returns entries as though only one toc existed (i.e. to hide
    // the mechanism of subtocs).
    // readMasked=true will walk subtocs and read indexes even if they are masked. This is
    // useful for dumping indexes which are cleared, or only referred to in cleared subtocs.
    bool readNext(TocRecord& r, bool walkSubTocs = true, bool hideSubTocEntries = true, bool hideClearEntries = true,
                  bool readMasked = false, const TocRecord** data = nullptr, size_t* length = nullptr) const;

    void selectSubTocRead(const eckit::LocalPathName& path) const;

    bool readNextInternal(TocRecord& r, const TocRecord** data = nullptr, size_t* length = nullptr) const;

    std::string userName(long) const;

    static size_t recordRoundSize();

    void dumpTocCache() const;

private:  // members

    eckit::LocalPathName tocPath_;
    Config dbConfig_;

    TocSerialisationVersion serialisationVersion_;

    bool useSubToc_;
    bool isSubToc_;
    bool preloadBTree_;

    // If we have mounted another TocCatalogue internally, what is the current
    // remapping key?
    Key remapKey_;

    mutable int fd_;  ///< file descriptor, if zero file is not yet open.

    mutable TocCopyWatcher tocReadStats_;
    mutable std::unique_ptr<eckit::MemoryHandle> cachedToc_;  ///< this is only for read path

    /// The sub toc is initialised in the read or write pathways for maintaining state.
    mutable std::map<eckit::LocalPathName, std::unique_ptr<TocHandler>> subTocReadCache_;
    mutable TocHandler* subTocRead_;  // n.b. non-owning
    mutable std::unique_ptr<TocHandler> subTocWrite_;
    mutable size_t count_;

    mutable std::set<std::pair<eckit::LocalPathName, eckit::Offset>> maskedEntries_;

    mutable bool enumeratedMaskedEntries_;
    mutable int numSubtocsRaw_;
    mutable bool writeMode_;

    mutable bool dirty_;
};


//-----------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb_TocHandler_H
