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
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/rados/RadosCatalogue.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"

#include <cstddef>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB writer that implements the FDB on Rados.
/// Not thread-safe: archive/flush/close calls on a single instance must be serialised by the caller.

class RadosCatalogueWriter : public RadosCatalogue, public CatalogueWriter {

public:  // methods

    RadosCatalogueWriter(const Key& key, const fdb5::Config& config);
    RadosCatalogueWriter(const eckit::URI& uri, const fdb5::Config& config);
    virtual ~RadosCatalogueWriter() override;

    void index(const Key& key, const eckit::URI& uri, eckit::Offset offset, eckit::Length length) override { NOTIMP; };

    void reconsolidate() override { NOTIMP; }

    void overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) override {
        NOTIMP;
    };

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

    void closeIndexes();

private:  // types

    using IndexStore = std::map<Key, Index>;

private:  // members

    IndexStore indexes_;

    Index current_;

    bool firstIndexWrite_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
