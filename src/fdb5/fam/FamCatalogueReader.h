// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   FamCatalogueReader.h
/// @author Metin Cakircali
/// @date   Mar 2026

#pragma once

#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/database/Index.h"
#include "fdb5/fam/FamCatalogue.h"

#include "eckit/filesystem/URI.h"

#include <cstddef>
#include <iosfwd>
#include <map>
#include <optional>
#include <string>

namespace fdb5 {

class Key;
class Field;

//----------------------------------------------------------------------------------------------------------------------

/// Read-only access to a FAM-backed FDB catalogue.
class FamCatalogueReader : public FamCatalogue, public CatalogueReader {

public:  // methods

    FamCatalogueReader(const Key& key, const Config& config);
    FamCatalogueReader(const eckit::URI& uri, const Config& config);

    DbStats stats() const override;

    bool selectIndex(const Key& key) override;
    void deselectIndex() override;

    bool open() override;
    void flush(size_t archived_fields) override {}
    void clean() override {}
    void close() override {}

    bool retrieve(const Key& key, Field& field) const override;

    void print(std::ostream& out) const override { out << "FamCatalogueReader[" << uri() << "]"; }

private:  // methods

    void dumpSchema(std::ostream& stream) const override;

    std::optional<Axis> computeAxis(const std::string& keyword) const override;

private:  // types

    using IndexStore = std::map<Key, Index>;

private:  // members

    IndexStore indexes_;
    Index current_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
