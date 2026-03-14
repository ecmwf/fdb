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

/// @file   FamCommon.h
/// @author Metin Cakircali
/// @date   Jun 2024

#pragma once

#include <string>

#include "eckit/io/fam/FamMap.h"
#include "eckit/io/fam/FamPath.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/serialisation/MemoryStream.h"

namespace fdb5 {

class Key;
class Config;

//----------------------------------------------------------------------------------------------------------------------

struct FamCommon {

    using Map = eckit::FamMap128;

    static constexpr auto type = eckit::FamPath::scheme;

    static constexpr const char* db_key = "__fdb__";

    static constexpr const char* registry_name = "__fdb-reg__";

    /// Suffix appended to every FAM map table name. Must match eckit::FamMap.
    static constexpr const char* table_suffix = "-map-table";

    static std::string toString(const Key& key);

    static Key decodeKey(eckit::MemoryStream key);

    // rules

    FamCommon(const FamCommon&)            = delete;
    FamCommon& operator=(const FamCommon&) = delete;
    FamCommon(FamCommon&&)                 = delete;
    FamCommon& operator=(FamCommon&&)      = delete;

    explicit FamCommon(eckit::FamRegionName root);

    explicit FamCommon(const eckit::URI& root);

    FamCommon(const Key& key, const Config& config);

    FamCommon(const eckit::URI& uri, const Config& config);

    virtual ~FamCommon() = default;

    // methods

    bool exists() const;

    eckit::URI uri() const;

    eckit::FamRegionName root_;

private:

    explicit FamCommon(const Config& config);
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
