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

/// @file   FamStore.h
/// @author Metin Cakircali
/// @date   Jun 2024

#pragma once

#include <cstddef>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/fam/FamObjectName.h"

#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Store.h"
#include "fdb5/fam/FamCommon.h"

namespace fdb5 {

class Schema;
class StoreWipeState;

//----------------------------------------------------------------------------------------------------------------------

class FamStore : protected FamCommon, public Store {

private:  // types

    // NOTE: Stats is mutated from const methods (retrieve) and is not thread-safe.
    struct Stats {
        size_t archived{0};
        size_t retrieved{0};
    };

public:  // methods

    FamStore(const Key& key, const Config& config);
    FamStore(const eckit::URI& uri, const Config& config);

    FamStore(const FamStore&)            = delete;
    FamStore& operator=(const FamStore&) = delete;
    FamStore(FamStore&&)                 = delete;
    FamStore& operator=(FamStore&&)      = delete;

    ~FamStore() override;

    std::string type() const override { return FamCommon::type; }

    static eckit::URI uri(const eckit::URI& dataURI);

    eckit::URI uri() const override;

    bool uriBelongs(const eckit::URI& uri) const override;

    bool uriExists(const eckit::URI& uri) const override;

    std::set<eckit::URI> collocatedDataURIs() const override;

    std::set<eckit::URI> asCollocatedDataURIs(const std::set<eckit::URI>& uris) const override;

    std::vector<eckit::URI> getAuxiliaryURIs(const eckit::URI& uri, bool onlyExisting) const override;

    bool open() override { return true; }

    size_t flush() override;

    void close() override;

    void checkUID() const override {}

    void finaliseWipeState(StoreWipeState& storeState, bool doit, bool unsafeWipeAll) override;

    bool doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const override;

    bool doWipeURIs(const StoreWipeState& wipeState) const override;

    void doWipeEmptyDatabase() const override;

    bool doUnsafeFullWipe() const override { return false; }

    eckit::FamObjectName makeObject(const Key& key) const;

protected:  // methods

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;

    std::unique_ptr<const FieldLocation> archive(const Key& key, const void* data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream& out) const override;

private:  // members

    mutable Stats stats_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
