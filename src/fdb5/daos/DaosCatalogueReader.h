// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @date Mar 2023

#pragma once

#include "fdb5/daos/DaosCatalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on DAOS

class DaosCatalogueReader : public DaosCatalogue, public CatalogueReader {

public:  // methods

    DaosCatalogueReader(const Key& key, const fdb5::Config& config);
    DaosCatalogueReader(const eckit::URI& uri, const fdb5::Config& config);

    DbStats stats() const override { NOTIMP; }

    bool selectIndex(const Key& idxKey) override;
    void deselectIndex() override;

    bool open() override;
    void flush(size_t archivedFields) override {}
    void clean() override {}
    void close() override {}

    bool retrieve(const Key& key, Field& field) const override;

    void print(std::ostream& out) const override { NOTIMP; }

private:  // types

    std::optional<Axis> computeAxis(const std::string& keyword) const override;
    typedef std::map<Key, Index> IndexStore;

private:  // members

    IndexStore indexes_;
    Index current_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
