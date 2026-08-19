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

#include "fdb5/rados/RadosCatalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on Rados

class RadosCatalogueWriter : public RadosCatalogue, public CatalogueWriter {

public:  // methods

    RadosCatalogueWriter(const Key& key, const fdb5::Config& config);
    RadosCatalogueWriter(const eckit::URI& uri, const fdb5::Config& config);
    virtual ~RadosCatalogueWriter() override;

    void index(const Key& key, const eckit::URI& uri, eckit::Offset offset, eckit::Length length) override { NOTIMP; };

    void reconsolidate() override { NOTIMP; }

    /// Mount an existing TocCatalogue, which has a different metadata key (within
    /// constraints) to allow on-line rebadging of data
    /// variableKeys: The keys that are allowed to differ between the two DBs
    void overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) override {
        NOTIMP;
    };

    const Index& currentIndex() override;

protected:  // methods

    virtual bool selectIndex(const Key& key) override;
    bool createIndex(const Key& idxKey, size_t datumKeySize) override;
    virtual void deselectIndex() override;

    bool open() override { NOTIMP; }
    void flush(size_t archivedFields) override;
    void clean() override;
    void close() override;

    void archive(const Key& idxKey, const Key& datumKey, std::shared_ptr<const FieldLocation> fieldLocation) override;

    void print(std::ostream& out) const override { out << "RadosCatalogueWriter(" << uri() << ")"; }

private:  // methods

    void closeIndexes();

private:  // types

    typedef std::map<Key, Index> IndexStore;

private:  // members

    IndexStore indexes_;

    Index current_;

    bool firstIndexWrite_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
