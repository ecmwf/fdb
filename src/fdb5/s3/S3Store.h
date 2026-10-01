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
/// @author Metin Cakircali
/// @author Simon Smart
/// @date   Feb 2024

#pragma once

#include "eckit/filesystem/URI.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Store.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/s3/S3Common.h"

#include <memory>
#include <set>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class S3Store : public Store, private S3Common {
public:  // methods

    S3Store(const Config& config);

    S3Store(const Key& key, const Config& config);

    S3Store(const eckit::URI& uri, const Config& config);

    ~S3Store() override = default;

    std::string type() const override { return "s3"; }

    eckit::URI uri() const override;

    static eckit::URI uri(const eckit::URI& dataURI);

    bool uriBelongs(const eckit::URI& uri) const override;

    bool uriExists(const eckit::URI& uri) const override;

    std::set<eckit::URI> collocatedDataURIs() const override;

    std::set<eckit::URI> asCollocatedDataURIs(const std::set<eckit::URI>& uris) const override;

    std::vector<eckit::URI> getAuxiliaryURIs(const eckit::URI& uri, bool onlyExisting) const override;

    bool open() override { return true; }

    size_t flush() override;

    void close() override;

    void checkUID() const override { /* nothing to do */ }

    /// Given a StoreWipeState from the Catalogue, identify URIs to be wiped
    void finaliseWipeState(StoreWipeState& storeState, bool doit, bool unsafeWipeAll) override;

    /// Delete unknown URIs. Part of an --unsafe-wipe-all operation.
    bool doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const override;

    /// Delete URIs marked in the wipe state
    bool doWipeURIs(const StoreWipeState& wipeState) const override;

    /// Delete empty DBs
    void doWipeEmptyDatabase() const override;

    /// Delete full DB in a single or a few operations
    bool doUnsafeFullWipe() const override;


private:  // methods

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;

    std::unique_ptr<const FieldLocation> archive(const Key& key, const void* data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream& out) const override;

    eckit::URI getAuxiliaryURI(const eckit::URI& uri, const std::string& ext) const;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
