/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   FamCatalogueReader.h
/// @author Metin Cakircali
/// @date   Mar 2026

#pragma once

#include <map>
#include <optional>

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/fam/FamCatalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Read-only access to a FAM-backed FDB catalogue.
class FamCatalogueReader : public FamCatalogue, public CatalogueReader {

public:  // methods

    FamCatalogueReader(const Key& key, const fdb5::Config& config);
    FamCatalogueReader(const eckit::URI& uri, const fdb5::Config& config);

    DbStats stats() const override { NOTIMP; }

    bool selectIndex(const Key& key) override;
    void deselectIndex() override;

    bool open() override;
    void flush(size_t archived_fields) override {}
    void clean() override {}
    void close() override {}

    bool retrieve(const Key& key, Field& field) const override;

    void print(std::ostream& out) const override { out << "FamCatalogueReader[" << uri() << "]"; }

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
