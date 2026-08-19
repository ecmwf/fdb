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
