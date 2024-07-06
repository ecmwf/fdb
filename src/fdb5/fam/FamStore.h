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

#include "eckit/io/fam/FamObjectName.h"
#include "fdb5/database/Store.h"
#include "fdb5/fam/FamCommon.h"

namespace fdb5 {

class Schema;

//----------------------------------------------------------------------------------------------------------------------

class FamStore: public Store, public FamCommon {
public:  // methods
    FamStore(const Schema& schema, const Key& key, const Config& config);

    ~FamStore() override;

    auto uri() const -> eckit::URI override;

    bool uriBelongs(const eckit::URI& uri) const override;
    bool uriExists(const eckit::URI& uri) const override;

    std::vector<eckit::URI> collocatedDataURIs() const override;
    std::set<eckit::URI>    asCollocatedDataURIs(const std::vector<eckit::URI>& uriList) const override;

    bool open() override { return true; }

    void flush() override;

    void close() override;

    void checkUID() const override { }

    eckit::FamObjectName makeObject(const Key& key) const;

protected:  // methods
    std::string type() const override { return TYPE; }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;

    std::unique_ptr<FieldLocation> archive(const Key& key, const void* data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream& out) const override;

private:  // members
    const Config& config_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
