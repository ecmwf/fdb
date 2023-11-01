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
/// @date   Nov 2022

#pragma once

#include "fdb5/database/Store.h"
#include "fdb5/rules/Schema.h"

#include "fdb5/daos/DaosCommon.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosStore : public Store, public DaosCommon {

public: // methods

    DaosStore(const Schema& schema, const Key& key, const Config& config);
    DaosStore(const Schema& schema, const eckit::URI& uri, const Config& config);

    ~DaosStore() override {}

    eckit::URI uri() const override;
    bool uriBelongs(const eckit::URI&) const override;
    bool uriExists(const eckit::URI&) const override;
    std::vector<eckit::URI> storeUnitURIs() const override;
    std::set<eckit::URI> asStoreUnitURIs(const std::vector<eckit::URI>&) const override;

    bool open() override { return true; }
    void flush() override;
    void close() override {};

    void checkUID() const override { /* nothing to do */ }

protected: // methods

    std::string type() const override { return "daos"; }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;
    std::unique_ptr<FieldLocation> archive(const Key &key, const void *data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream &out) const override;

private: // members

    const Config& config_;

    std::string db_str_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
