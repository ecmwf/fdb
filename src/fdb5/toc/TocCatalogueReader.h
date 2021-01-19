/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocCatalogueReader.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_TocCatalogueReader_H
#define fdb5_TocCatalogueReader_H

#include "fdb5/toc/TocCatalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocCatalogueReader : public TocCatalogue, public CatalogueReader {

public: // methods

    TocCatalogueReader(const Key& key, const fdb5::Config& config);
    TocCatalogueReader(const eckit::URI& uri, const fdb5::Config& config);

    ~TocCatalogueReader() override;

    std::vector<Index> indexes(bool sorted) const override;
    DbStats stats() const override { return TocHandler::stats(); }

    bool archiveLocked() const override { return false; }
    bool wipeLocked() const override { return false; }

private: // methods

    void loadIndexesAndRemap();
    bool selectIndex(const Key &key) override;
    void deselectIndex() override;

    bool open() override;
    void flush() override {}
    void clean() override {}
    void close() override;

    void axis(const std::string &keyword, eckit::StringSet &s) const override;

    bool retrieve(const Key& key, Field& field) const override;

    void print( std::ostream &out ) const override;

private: // members

    // Indexes matching current key. If there is a key remapping for a mounted
    // SubToc, then this is stored alongside
    std::vector<std::pair<Index, Key>> matching_;

    // All indexes
    // If there is a key remapping for a mounted SubToc, this is stored alongside
    std::vector<std::pair<Index, Key>> indexes_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
