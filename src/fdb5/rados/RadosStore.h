/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Emanuele Danovaro
/// @author Nicolau Manubens
/// @date   Feb 2024

#pragma once

#include "fdb5/config/Config.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Store.h"
#include "fdb5/rados/RadosCommon.h"
#include "fdb5/rules/Schema.h"

#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/rados/RadosObject.h"

#include <cstddef>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Store that implements the FDB on CEPH object store

class RadosStore : public Store, public RadosCommon {

public:  // methods

    RadosStore(const Key& key, const Config& config);
    RadosStore(const Schema& schema, const Key& key, const Config& config);
    RadosStore(const eckit::URI& uri, const Config& config);

    eckit::URI uri() const override;
    static eckit::URI uri(const eckit::URI& dataURI);
    bool uriBelongs(const eckit::URI&) const override;
    bool uriExists(const eckit::URI&) const override;
    std::set<eckit::URI> collocatedDataURIs() const override;
    std::set<eckit::URI> asCollocatedDataURIs(const std::set<eckit::URI>&) const override;

    bool open() override { return true; }
    size_t flush() override;
    void close() override;

    void checkUID() const override { /* nothing to do */ }

    /// Wipe-related methods (not implemented for the Rados backend)
    void finaliseWipeState(StoreWipeState& storeState, bool doit, bool unsafeWipeAll) override;
    bool doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const override;
    bool doWipeURIs(const StoreWipeState& wipeState) const override;
    void doWipeEmptyDatabase() const override;
    bool doUnsafeFullWipe() const override;

    std::vector<eckit::URI> getAuxiliaryURIs(const eckit::URI& uri, bool onlyExisting) const override;

protected:  // methods

    std::string type() const override { return "rados"; }
    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;
    std::unique_ptr<const FieldLocation> archive(const Key& key, const void* data, eckit::Length length) override;

    using Store::remove;
    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream& out) const override;

    eckit::RadosObject generateDataObject(const Key& key) const;

    const eckit::RadosObject& getDataObject(const Key& key) const;
    eckit::DataHandle& getDataHandle(const Key& key, const eckit::RadosObject& name);
    void closeDataHandles();
    void flushDataHandles();

private:  // types

    using HandleStore = std::map<Key, eckit::DataHandle*>;
    using ObjectStore = std::map<Key, eckit::RadosObject>;

private:  // members

    size_t archivedFields_{0};

    HandleStore handles_;
    mutable ObjectStore dataObjects_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
