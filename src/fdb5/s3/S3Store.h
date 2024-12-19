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
#include "eckit/io/s3/S3ObjectName.h"
#include "fdb5/database/Store.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/s3/S3Common.h"

#include <set>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class S3Store : public Store, public S3Common {

public:  // methods
    S3Store(const Schema& schema, const Key& key, const Config& config);

    S3Store(const Schema& schema, const eckit::URI& uri, const Config& config);

    ~S3Store() override { }

    eckit::URI uri() const override;

    bool uriBelongs(const eckit::URI& uri) const override;

    bool uriExists(const eckit::URI& uri) const override;

    std::vector<eckit::URI> collocatedDataURIs() const override;

    std::set<eckit::URI> asCollocatedDataURIs(const std::vector<eckit::URI>& uris) const override;

    std::vector<eckit::URI> getAuxiliaryURIs(const eckit::URI& uri) const override;

    bool auxiliaryURIExists(const eckit::URI& uri) const override;

    bool open() override { return true; }

    void flush() override;

    void close() override;

    void checkUID() const override { /* nothing to do */ }

private:  // methods
    std::string type() const override { return "s3"; }

    bool exists() const override;

    eckit::DataHandle*                   retrieve(Field& field) const override;
    std::unique_ptr<const FieldLocation> archive(const Key& key, const void* data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream& out) const override;

    eckit::S3ObjectName generateDataKey(const Key& key) const;

    eckit::URI getAuxiliaryURI(const eckit::URI& uri, const std::string& ext) const;

private:  // members
    const Config& config_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
