// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @author Metin Cakircali
/// @date Jun 2024

#pragma once

#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/rados/RadosCatalogue.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"

#include <cstddef>
#include <memory>
#include <ostream>
#include <set>
#include <string>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB writer that implements the FDB on Rados.
/// Not thread-safe per instance; separate writer instances may archive to the same database.
/// Nothing is written before flush(); entries still queued on destruction are dropped, as the data they refer
/// to may not have been flushed either.

class RadosCatalogueWriter : public RadosCatalogue, public CatalogueWriter {

public:  // methods

    RadosCatalogueWriter(const Key& key, const fdb5::Config& config);
    RadosCatalogueWriter(const eckit::URI& uri, const fdb5::Config& config);

    void index(const Key& key, const eckit::URI& uri, eckit::Offset offset, eckit::Length length) override { NOTIMP; };

    void reconsolidate() override { NOTIMP; }

    void overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) override {
        NOTIMP;
    };

    void hideContents() override;

    const Index& currentIndex() override;

protected:  // methods

    bool selectIndex(const Key& key) override;
    bool createIndex(const Key& idxKey, size_t datumKeySize) override;
    void deselectIndex() override;

    bool open() override { NOTIMP; }
    void flush(size_t archivedFields) override;
    void clean() override;
    void close() override;

    void archive(const Key& idxKey, const Key& datumKey, std::shared_ptr<const FieldLocation> fieldLocation) override;

    void print(std::ostream& out) const override { out << "RadosCatalogueWriter(" << uri() << ")"; }

private:  // methods

    // Writes the catalogue KV (schema, key) and registers the database in the root KV.
    void createDatabase();

private:  // members

    // False for a new database until the first flush() has written and registered its catalogue KV.
    bool created_{true};

    // Schema of a database which is not created yet, as read from the configured schema file.
    std::string schemaText_;

    // Keys of new indexes which are not yet registered in the catalogue KV.
    std::set<Key> unregistered_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
