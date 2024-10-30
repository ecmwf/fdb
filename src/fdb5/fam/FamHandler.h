/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   FamHandler.h
/// @author Metin Cakircali
/// @date   Jul 2024

#pragma once

#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/log/Timer.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamRecord.h"
#include "fdb5/fam/FamRecordVersion.h"

#include <map>
#include <memory>

namespace eckit {
class Configuration;
}

namespace fdb5 {

class Key;
class Index;

//----------------------------------------------------------------------------------------------------------------------

extern const std::map<ControlIdentifier, const char*> controlfile_lookup;

//----------------------------------------------------------------------------------------------------------------------

// class FamCopyWatcher: public eckit::TransferWatcher {
// void fromHandleOpened() override {
// timer_.start();
// time_ = timer_.elapsed();
// }
//
// void toHandleOpened() override {
// timer_.start();
// time_ = timer_.elapsed();
// }
//
// void watch(const void*, long len) override {
// double now = timer_.elapsed();
// FamCopyStats_.push_back(std::make_pair(now - time_, len));
// time_ = now;
// }
//
// public:
// FamCopyWatcher(): idx_(0) { }
//
// size_t size() const { return FamCopyStats_.size(); }
//
// bool next(double& time, eckit::Length& len) {
// if (FamCopyStats_.size() > idx_) {
// time = FamCopyStats_[idx_].first;
// len  = FamCopyStats_[idx_].second;
//
// idx_++;
// return true;
// }
// return false;
// }
//
// private:
// eckit::Timer timer_;
// double       time_;
//
// size_t                                        idx_;
// std::vector<std::pair<double, eckit::Length>> FamCopyStats_;
// };

//----------------------------------------------------------------------------------------------------------------------

class FamHandler: private eckit::NonCopyable, protected FamCommon {
public:  // typedefs
    using FamVec   = std::vector<FamRecord>;
    using FamPaths = std::vector<eckit::PathName>;

public:  // methods
    FamHandler(const Key& key, const Config& config);

    FamHandler(const eckit::FamRegionName& region, const Config& config);

    /// @note used for subtocs
    FamHandler(const eckit::FamRegionName& region, const Key& parentKey);

    ~FamHandler();

    bool exists() const;

    void writeInitRecord(const Key& FamKey);
    void writeClearRecord(const Index&);
    void writeClearAllRecord();
    void writeSubTocRecord(const FamHandler& SubToc);
    void writeIndexRecord(const Index&);
    void writeSubTocMaskRecord(const FamHandler& SubToc);

    bool useSubToc() const;
    bool anythingWrittenToSubToc() const;

    /// Return a list of existent indexes. If supplied, also supply a list of associated
    /// SubTocs that were read to get these indexes
    std::vector<Index> loadIndexes(bool                   sorted        = false,
                                   std::set<std::string>* subTocs       = nullptr,
                                   std::vector<bool>*     indexInSubToc = nullptr,
                                   std::vector<Key>*      remapKeys     = nullptr) const;

    Key    databaseKey();
    size_t numberOfRecords() const;

    // auto root() const -> const eckit::FamRegionName& { NOTIMP; }

    // const eckit::PathName& directory() const;
    const eckit::PathName& famPath() const;
    const eckit::PathName& schemaPath() const;

    void dump(std::ostream& out, bool simple = false, bool walk = true) const;
    void dumpIndex(std::ostream& out, const eckit::FamObjectName& index) const;

    // std::string dbOwner() const;
    // DbStats stats() const;

    void enumerateMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata, std::set<eckit::URI>& data) const;

    std::vector<eckit::PathName> subTocPaths() const;

    // Utilities for handling locks
    std::vector<eckit::PathName> lockfilePaths() const;

protected:  // methods
    size_t FamFilesSize() const;

    // Access and control of locks

    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const;

    bool enabled(const ControlIdentifier& controlIdentifier) const;

private:  // methods
    auto fullControlFilePath(const std::string& name) const -> eckit::PathName;

    void createControlFile(const std::string& name) const;

    void removeControlFile(const std::string& name) const;

protected:  // members
    // Contains the key of the first toc explored in SubToc chain
    mutable Key parentKey_;

protected:  // methods
    // Handle location and remapping information if using a mounted FamCatalogue

    auto currentRoot() const -> const eckit::FamRegionName&;

    auto currentToc() const -> const eckit::FamObjectName&;

    auto currentRemapKey() const -> const Key&;

    // Build the record, and return the payload size

    static size_t buildIndexRecord(FamRecord& record, const Index& index);
    static size_t buildClearRecord(FamRecord& record, const Index& index);
    size_t        buildSubTocMaskRecord(FamRecord& record);
    static size_t buildSubTocMaskRecord(FamRecord& record, const eckit::PathName& path);

    // Given the payload size, returns the record size

    static size_t roundRecord(FamRecord& record, size_t payloadSize);

    void appendBlock(const void* data, size_t size);

    auto recordVersion() const -> const FamRecordVersion& { return version_; }

private:  // typedefs
    class AutoCloser;

    friend AutoCloser;

    enum class Mode { CLOSED, READ, WRITE };

    using Entries = std::set<std::pair<eckit::PathName, eckit::Offset>>;

private:  // methods
    static size_t recordRoundSize();

    void open(Mode mode) const;

    void openForAppend();

    void openForRead() const;

    void close() const;

    /// Populate the masked sub toc list, starting from the _current_position_ in the
    /// file (opened for read). It resets back to the same place when done. This is
    /// to allow searching only from the first SubToc.
    void allMaskableEntries(eckit::Offset startOffset, eckit::Offset endOffset, Entries& maskedEntries) const;

    void populateMaskedEntriesList() const;

    void append(FamRecord& record, size_t payloadSize);

    // hideSubTocEntries=true returns entries as though only one toc existed (i.e. to hide
    // the mechanism of SubTocs).
    // readMasked=true will walk SubTocs and read indexes even if they are masked. This is
    // useful for dumping indexes which are cleared, or only referred to in cleared SubTocs.
    bool readNext(FamRecord& record,
                  bool       walkSubTocs       = true,
                  bool       hideSubTocEntries = true,
                  bool       hideClearEntries  = true,
                  bool       readMasked        = false) const;

    bool readNextInternal(FamRecord& record) const;

    void dumpCache() const;

private:  // members
    // const Config config_;

    eckit::FamObjectName handler_;

    const FamRecordVersion version_;

    bool isSubToc_ {false};

    bool useSubToc_ {false};
    // bool preloadBTree_;

    Key remapKey_;

    mutable size_t count_ {0};

    mutable bool enumerated_ {false};

    mutable Mode mode_ {Mode::CLOSED};

    /// The sub Fam is initialised in the read or write pathways for maintaining state.
    mutable std::unique_ptr<FamHandler> subTocRead_;
    mutable std::unique_ptr<FamHandler> subTocWrite_;

    mutable std::unique_ptr<eckit::MemoryHandle> cached_;  // only for read path
    // mutable FamCopyWatcher                       FamReadStats_;

    mutable Entries masked_;
};

//-----------------------------------------------------------------------------

}  // namespace fdb5
