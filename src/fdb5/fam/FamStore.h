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

//----------------------------------------------------------------------------------------------------------------------

class FamStore : protected FamCommon, public Store {

private:  // types

    struct Stats {
        size_t archived{0};
        size_t retrieved{0};
    };


public:  // methods

    FamStore(const Key& key, const Config& config);

    ~FamStore() override;

    auto type() const -> std::string override { return FamCommon::type; }

    auto uri() const -> eckit::URI override;

    auto uriBelongs(const eckit::URI& uri) const -> bool override;

    auto uriExists(const eckit::URI& uri) const -> bool override;

    auto collocatedDataURIs() const -> std::vector<eckit::URI> override;

    auto asCollocatedDataURIs(const std::vector<eckit::URI>& uriList) const -> std::set<eckit::URI> override;

    auto open() -> bool override { return true; }

    size_t flush() override;

    void close() override;

    void checkUID() const override {}

    auto makeObject(const Key& key) const -> eckit::FamObjectName;

protected:  // methods

    auto exists() const -> bool override;

    auto retrieve(Field& field) const -> eckit::DataHandle* override;

    auto archive(const Key& key, const void* data, eckit::Length length)
        -> std::unique_ptr<const FieldLocation> override;

    void remove(const Key& key) const override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream& out) const override;

private:  // members

    mutable Stats stats_;

    const Config& config_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
