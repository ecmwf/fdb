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
/// @date Mar 2023

#pragma once

#include "fdb5/daos/DaosCatalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on DAOS

class DaosCatalogueReader : public DaosCatalogue, public CatalogueReader {

public: // methods

    DaosCatalogueReader(const Key& key, const fdb5::Config& config);
    DaosCatalogueReader(const eckit::URI& uri, const fdb5::Config& config);

//     ~TocCatalogueReader() override;

//     std::vector<Index> indexes(bool sorted) const override;
//     DbStats stats() const override { return TocHandler::stats(); }
    DbStats stats() const override { NOTIMP; }

// private: // methods

//     void loadIndexesAndRemap();
    bool selectIndex(const Key &key) override;
    void deselectIndex() override;

    bool open() override;
    void flush() override {}
    void clean() override {}
    void close() override {}
    
    void axis(const std::string &keyword, eckit::StringSet &s) const override;

    bool retrieve(const Key& key, Field& field) const override;

    void print( std::ostream &out ) const override { NOTIMP; }

private: // types

    typedef std::map< Key, Index> IndexStore;

private: // members

//     // Indexes matching current key. If there is a key remapping for a mounted
//     // SubToc, then this is stored alongside
//     std::vector<std::pair<Index, Key>> matching_;

//     // All indexes
//     // If there is a key remapping for a mounted SubToc, this is stored alongside
//     std::vector<std::pair<Index, Key>> indexes_;
    IndexStore indexes_;
    Index current_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
