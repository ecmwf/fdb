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

private: // methods

    void loadIndexesAndRemap() const;
    bool selectIndex(const Key &key) override;
    void deselectIndex() override;

    bool open() override;
    void flush() override {}
    void clean() override {}
    void close() override;
    
    bool axis(const std::string &keyword, eckit::DenseSet<std::string>& s) const override;

    bool retrieve(const Key& key, Field& field) const override;

    void print( std::ostream &out ) const override;

    const std::vector<std::pair<Index, Key>>& mappedIndexes() const;
    std::vector<std::pair<Index, Key>>& mappedIndexes();

private: // members

    using index_list_t = std::vector<std::pair<Index, Key>*>;

    // Indexes matching current key. If there is a key remapping for a mounted
    // SubToc, then this is stored alongside
    index_list_t matching_;

    // A lookup for further refined detalis, if we can go beyond the current set of matching indexes
    std::map<Key, index_list_t> refinedMatching_;

    // All indexes
    // If there is a key remapping for a mounted SubToc, this is stored alongside
    mutable std::vector<std::pair<Index, Key>> indexes_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
