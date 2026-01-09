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

public:  // methods

    DaosStore(const Key& key, const Config& config);
    DaosStore(const eckit::URI& uri, const Config& config);

    ~DaosStore() override {}

    eckit::URI uri() const override;
    static eckit::URI uri(const eckit::URI& dataURI);
    bool uriBelongs(const eckit::URI&) const override;
    bool uriExists(const eckit::URI&) const override;
    std::set<eckit::URI> collocatedDataURIs() const override;
    std::set<eckit::URI> asCollocatedDataURIs(const std::set<eckit::URI>&) const override;

    bool open() override { return true; }
    size_t flush() override;
    void close() override {};

    void checkUID() const override { /* nothing to do */ }

    void prepareWipe(StoreWipeState& storeState, bool doit, bool unsafeWipeAll) override;
    bool doWipeUnknownContents(const std::set<eckit::URI>& unknownURIs) const override;
    bool doWipe(const StoreWipeState& wipeState) const override;
    void doWipeEmptyDatabases() const override;
    bool doUnsafeFullWipe() const override;

    // DAOS store does not currently support auxiliary objects
    std::vector<eckit::URI> getAuxiliaryURIs(const eckit::URI&, bool onlyExisting = false) const override { return {}; }

protected:  // methods

    std::string type() const override { return "daos"; }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;
    std::unique_ptr<const FieldLocation> archive(const Key& key, const void* data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream& out) const override;

private:  // members

    size_t archivedFields_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
