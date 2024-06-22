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

class RadosCatalogueReader : public RadosCatalogue, public CatalogueReader {

public: // methods

    RadosCatalogueReader(const Key& key, const fdb5::Config& config);
    RadosCatalogueReader(const eckit::URI& uri, const fdb5::Config& config);

    DbStats stats() const override { NOTIMP; }

    bool selectIndex(const Key &key) override;
    void deselectIndex() override;

    bool open() override;
    void flush() override {}
    void clean() override {}
    void close() override {}
    
    bool axis(const std::string &keyword, eckit::StringSet &s) const override;

    bool retrieve(const Key& key, Field& field) const override;

    void print( std::ostream &out ) const override { NOTIMP; }

private: // types

    typedef std::map< Key, Index> IndexStore;

private: // members

    IndexStore indexes_;
    Index current_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
