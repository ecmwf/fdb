// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Emanuele Danovaro
/// @author Nicolau Manubens
/// @author Metin Cakircali
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

/// Store that implements the FDB on CEPH object store.
/// Not thread-safe: archive/flush/close calls on a single instance must be serialised by the caller.

class RadosStore : public Store, public RadosCommon {

public:  // methods

    RadosStore(const Key& key, const Config& config);
    RadosStore(const Schema& schema, const Key& key, const Config& config);
    RadosStore(const eckit::URI& uri, const Config& config);
    ~RadosStore() override;

    RadosStore(const RadosStore&) = delete;
    RadosStore& operator=(const RadosStore&) = delete;
    RadosStore(RadosStore&&) = delete;
    RadosStore& operator=(RadosStore&&) = delete;

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

    // Wipe-related methods
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

    void closeDataHandles();
    void flushDataHandles();

private:  // types

    // The data object fields of one index key are appended to, and the handle writing to it.
    struct DataUnit {
        eckit::RadosObject object;
        std::unique_ptr<eckit::DataHandle> handle;
    };

    using DataUnits = std::map<Key, DataUnit>;

private:  // methods

    eckit::RadosObject generateDataObject(const Key& key) const;

    // The unit for `key`, created with a new object and an open handle on first use.
    DataUnit& dataUnit(const Key& key);

private:  // members

    size_t archivedFields_{0};

    DataUnits units_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
