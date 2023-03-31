/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
/// @date Feb 2023

#pragma once

// #include "eckit/os/AutoUmask.h"

// #include "fdb5/database/Index.h"
// #include "fdb5/toc/TocRecord.h"

#include "fdb5/daos/DaosCatalogue.h"

// #include "fdb5/toc/TocSerialisationVersion.h"

namespace fdb5 {

// class Key;
// class TocAddIndex;

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on DAOS

class DaosCatalogueWriter : public DaosCatalogue, public CatalogueWriter {

public: // methods

    DaosCatalogueWriter(const Key &key, const fdb5::Config& config);
    DaosCatalogueWriter(const eckit::URI& uri, const fdb5::Config& config);

    virtual ~DaosCatalogueWriter() override;

//     /// Used for adopting & indexing external data to the TOC dir
    void index(const Key &key, const eckit::URI &uri, eckit::Offset offset, eckit::Length length) override { NOTIMP; };

    // void reconsolidate() override { reconsolidateIndexesAndTocs(); }
    void reconsolidate() override { NOTIMP; }

//     /// Mount an existing TocCatalogue, which has a different metadata key (within
//     /// constraints) to allow on-line rebadging of data
//     /// variableKeys: The keys that are allowed to differ between the two DBs
    void overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) override { NOTIMP; };

//     // Hide the contents of the DB!!!
//     void hideContents() override;

//     bool enabled(const ControlIdentifier& controlIdentifier) const override;

    const Index& currentIndex() override;
//     const TocSerialisationVersion& serialisationVersion() const;

protected: // methods

    virtual bool selectIndex(const Key &key) override;
    virtual void deselectIndex() override;

    bool open() override { NOTIMP; }
    void flush() override;
    void clean() override;
    void close() override;

    void archive(const Key& key, const FieldLocation* fieldLocation) override;
//     void reconsolidateIndexesAndTocs();

    virtual void print( std::ostream &out ) const override { NOTIMP; }

private: // methods

    void closeIndexes();
//     void flushIndexes();
//     void compactSubTocIndexes();

//     eckit::PathName generateIndexPath(const Key &key) const;

private: // types

//     typedef std::map< std::string, eckit::DataHandle * >  HandleStore;
    typedef std::map< Key, Index> IndexStore;
//     typedef std::map< Key, std::string > PathStore;

private: // members

//     HandleStore handles_;    ///< stores the DataHandles being used by the Session

//     // If we have multiple flush statements, then the indexes get repeatedly reset. Build and maintain
//     // a full copy of the indexes associated with the process as well, for use when masking out
//     // subtocs. See compactSubTocIndexes.
    IndexStore  indexes_;
//     IndexStore  fullIndexes_;

//     PathStore   dataPaths_;

    Index current_;
    // Index currentFull_;

//     eckit::AutoUmask umask_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
