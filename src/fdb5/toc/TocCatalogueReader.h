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

#include <iosfwd>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/TocCatalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocCatalogueReader : public TocCatalogue, public CatalogueReader {
private:  // types
    using IndexKey  = std::pair<Index, Key>;
    using MapList   = std::vector<IndexKey>;
    using MatchList = std::vector<const IndexKey*>;

public: // methods

    TocCatalogueReader(const Key& dbKey, const fdb5::Config& config);
    TocCatalogueReader(const eckit::URI& uri, const fdb5::Config& config);

    ~TocCatalogueReader() override;

    std::vector<Index> indexes(bool sorted) const override;
    DbStats stats() const override { return TocHandler::stats(); }

private:  // methods
    void loadIndexesAndRemap() const;
    bool selectIndex(const Key& idxKey) override;
    void deselectIndex() override;

    bool open() override;
    void flush(size_t archivedFields) override {}
    void clean() override {}
    void close() override;

    bool axis(const std::string& keyword, eckit::DenseSet<std::string>& s) const override;

    bool retrieve(const Key& key, Field& field) const override;

    void print( std::ostream &out ) const override;

    template<class T>
    static auto& getOrMapIndexes(T& toc) {
        if (toc.indexes_.empty()) { toc.loadIndexesAndRemap(); }
        return toc.indexes_;
    }

    auto mappedIndexes() -> MapList& { return getOrMapIndexes(*this); }

    auto mappedIndexes() const -> const MapList& { return getOrMapIndexes(*this); }

private: // members

    // Indexes matching current key. If there is a key remapping for a mounted
    // SubToc, then this is stored alongside
    MatchList matching_;

    // A lookup for further refined details, if we can go beyond the current set of matching indexes
    mutable std::map<Key, MatchList> keyMatching_;

    // All indexes
    // If there is a key remapping for a mounted SubToc, this is stored alongside
    mutable MapList indexes_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
