// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @author Metin Cakircali
/// @date Jun 2024

#pragma once

#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/rados/RadosCatalogue.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"

#include <map>
#include <optional>
#include <ostream>
#include <string>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on Rados

class RadosCatalogueReader : public RadosCatalogue, public CatalogueReader {

public:  // methods

    RadosCatalogueReader(const Key& key, const fdb5::Config& config);
    RadosCatalogueReader(const eckit::URI& uri, const fdb5::Config& config);

    DbStats stats() const override;

    bool selectIndex(const Key& key) override;
    void deselectIndex() override;

    bool open() override;
    void flush(size_t archivedFields) override {}
    void clean() override {}
    void close() override {}

    bool retrieve(const Key& key, Field& field) const override;

    void print(std::ostream& out) const override { out << "RadosCatalogueReader(" << uri() << ")"; }

private:  // methods

    std::optional<Axis> computeAxis(const std::string& keyword) const override;

private:  // types

    using IndexStore = std::map<Key, Index>;

private:  // members

    IndexStore indexes_;
    Index current_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
