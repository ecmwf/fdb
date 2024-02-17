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
/// @date   Feb 2024

#pragma once

#include "eckit/io/s3/S3Name.h"

#include "fdb5/database/Store.h"
#include "fdb5/rules/Schema.h"

#include "fdb5/s3/S3Common.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class S3Store : public Store, public S3Common {

public: // methods

    S3Store(const Schema& schema, const Key& key, const Config& config);
    S3Store(const Schema& schema, const eckit::URI& uri, const Config& config);

    ~S3Store() override {}

    eckit::URI uri() const override;
    bool uriBelongs(const eckit::URI&) const override;
    bool uriExists(const eckit::URI&) const override;
    std::vector<eckit::URI> storeUnitURIs() const override;
    std::set<eckit::URI> asStoreUnitURIs(const std::vector<eckit::URI>&) const override;

    bool open() override { return true; }
    void flush() override;
    void close() override;

    void checkUID() const override { /* nothing to do */ }

protected: // methods

    std::string type() const override { return "s3"; }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;
    std::unique_ptr<FieldLocation> archive(const Key& key, const void * data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream &out) const override;

    eckit::S3Name generateDataKey(const Key& key) const;

    /// @note: code for S3 object (key) per index store:
    // eckit::S3Name getDataKey(const Key& key) const;
    // eckit::DataHandle& getDataHandle(const Key& key, const eckit::S3Name& name);
    // void closeDataHandles();

private: // types

    /// @note: code for S3 object (key) per index store:
    // typedef std::map<Key, eckit::DataHandle*> HandleStore;
    // typedef std::map<Key, std::string> KeyStore;

private: // members
    
    const Config& config_;

    /// @note: code for S3 object (key) per index store:
    // HandleStore handles_;
    // mutable KeyStore dataKeys_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5