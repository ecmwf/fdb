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

/// @file   FamCatalogueWriter.h
/// @author Metin Cakircali
/// @date   Mar 2026

#pragma once

#include <map>

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/fam/FamCatalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Write access to a FAM-backed FDB catalogue.
///
/// On construction the writer ensures the catalogue FamMap exists in the root
/// FAM region and registers the DB key in the global FDB registry map so that
/// FamEngine::visitableLocations can discover the database later.
class FamCatalogueWriter : public FamCatalogue, public CatalogueWriter {

public:  // methods

    FamCatalogueWriter(const Key& key, const fdb5::Config& config);
    FamCatalogueWriter(const eckit::URI& uri, const fdb5::Config& config);

    ~FamCatalogueWriter() override;

    void index(const Key& key, const eckit::URI& uri, eckit::Offset offset, eckit::Length length) override { NOTIMP; }

    void reconsolidate() override { NOTIMP; }

    void overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) override {
        NOTIMP;
    }

    const Index& currentIndex() override;

protected:  // methods

    bool selectIndex(const Key& idx_key) override;
    bool createIndex(const Key& idx_key, size_t datum_key_size) override;
    void deselectIndex() override;

    bool open() override { NOTIMP; }
    void flush(size_t archived_fields) override;
    void clean() override;
    void close() override;

    void archive(const Key& idx_key, const Key& datum_key, std::shared_ptr<const FieldLocation> fieldLocation) override;

    void print(std::ostream& out) const override;

private:  // methods

    void initCatalogue();

private:  // types

    using IndexStore = std::map<Key, Index>;

private:  // members

    IndexStore indexes_;
    Index current_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
