// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   TocCatalogueReader.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016
#pragma once
#include <cstddef>
#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "eckit/filesystem/URI.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/TocCatalogue.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocCatalogueReader : public TocCatalogue, public CatalogueReader {

public:  // methods

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

    bool retrieve(const Key& key, Field& field) const override;

    void print(std::ostream& out) const override;

private:  // members

    std::optional<Axis> computeAxis(const std::string& keyword) const override;

    /// Indexes matching current key. If there is a key remapping for a mounted
    /// SubToc, then this is stored alongside
    std::vector<std::pair<Index, Key>*> matching_;

    /// All indexes
    /// If there is a key remapping for a mounted SubToc, this is stored alongside
    mutable std::vector<std::pair<Index, Key>> indexes_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
