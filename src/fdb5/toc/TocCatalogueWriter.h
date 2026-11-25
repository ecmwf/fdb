// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   TocCatalogueWriter.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/toc/TocCatalogue.h"
#include "fdb5/toc/TocSerialisationVersion.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/os/AutoUmask.h"

#include <cstddef>
#include <iosfwd>
#include <mutex>
#include <set>
#include <string>

namespace fdb5 {

class Key;
class TocAddIndex;
class Catalogue;
class Config;

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocCatalogueWriter : public TocCatalogue, public CatalogueWriter {

public:  // methods

    TocCatalogueWriter(const Key& dbKey, const Config& config);
    TocCatalogueWriter(const eckit::URI& uri, const Config& config);

    ~TocCatalogueWriter() override;

    /// Used for adopting & indexing external data to the TOC dir
    void index(const Key& key, const eckit::URI& uri, eckit::Offset offset, eckit::Length length) override;

    void reconsolidate() override { reconsolidateIndexesAndTocs(); }

    /// Mount an existing TocCatalogue, which has a different metadata key (within
    /// constraints) to allow on-line rebadging of data
    /// variableKeys: The keys that are allowed to differ between the two DBs
    void overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) override;

    // Hide the contents of the DB!!!
    void hideContents() override;

    bool enabled(const ControlIdentifier& controlIdentifier) const override;

    const Index& currentIndex() override;
    const Key currentIndexKey() override;
    const TocSerialisationVersion& serialisationVersion() const;

    size_t archivedLocations() const override { return archivedLocations_; }

protected:  // methods

    bool selectIndex(const Key& idxKey) override;
    bool createIndex(const Key& idxKey, size_t datumKeySize) override;
    void deselectIndex() override;

    bool open() override;
    void flush(size_t archivedFields) override;
    void clean() override;
    void close() override;

    void archive(const Key& idxKey, const Key& datumKey, std::shared_ptr<const FieldLocation> fieldLocation) override;
    void reconsolidateIndexesAndTocs();

    void print(std::ostream& out) const override;

private:  // methods

    /// @note non-locking; callers must hold mutex_

    bool selectIndexUnlocked(const Key& idxKey);
    bool createIndexUnlocked(const Key& idxKey, size_t datumKeySize);
    void deselectIndexUnlocked();
    const Index& currentIndexUnlocked();

    void closeIndexes();
    void flushIndexes();
    void compactSubTocIndexes();

    eckit::PathName generateIndexPath(const Key& key) const;

    /// Create the access timestamps. Called only when this writer is what created the database.
    void createAccessTimestamps() const;

private:  // types

    typedef std::map<std::string, eckit::DataHandle*> HandleStore;
    typedef std::map<Key, Index> IndexStore;
    typedef std::map<Key, std::string> PathStore;

private:  // members

    HandleStore handles_;  ///< stores the DataHandles being used by the Session

    // If we have multiple flush statements, then the indexes get repeatedly reset. Build and maintain
    // a full copy of the indexes associated with the process as well, for use when masking out
    // subtocs. See compactSubTocIndexes.
    IndexStore indexes_;
    IndexStore fullIndexes_;

    PathStore dataPaths_;

    Index current_;
    Index currentFull_;

    eckit::AutoUmask umask_;
    size_t archivedLocations_;

    std::mutex mutex_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
