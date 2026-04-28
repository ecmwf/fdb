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

#include <mutex>
#include <optional>
#include <string>

#include "eckit/io/fam/FamMap.h"
#include "eckit/io/fam/FamRegion.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/io/fam/FamTypes.h"
#include "eckit/serialisation/MemoryStream.h"

namespace fdb5 {

class Key;
class Config;

//----------------------------------------------------------------------------------------------------------------------

struct FamCommon {

    using Map = eckit::FamMap128;

    static constexpr auto type = eckit::fam::scheme;

    static constexpr const char* db_keyword = "__fdb__";

    static constexpr const char* schema_keyword = "__schema__";

    static constexpr const char* registry_keyword = "__fdb-reg__";

    static constexpr const char* axes_keyword = "__axes__";

    static constexpr const char* catalogue_prefix = "c";

    static constexpr const char* index_prefix = "i";

    /// Suffix appended to every FAM map table name.
    static constexpr const char* table_suffix = Map::table_suffix;

    static std::string toString(const Key& key);

    /// Serialise a Key to a binary string (inverse of decodeKey).
    static std::string encodeKey(const Key& key);

    /// Deserialise a Key from a binary string (inverse of encodeKey).
    static Key decodeKey(eckit::MemoryStream key);

    // rules

    FamCommon(const FamCommon&) = delete;
    FamCommon& operator=(const FamCommon&) = delete;
    FamCommon(FamCommon&&) = delete;
    FamCommon& operator=(FamCommon&&) = delete;

    explicit FamCommon(eckit::FamRegionName root);

    explicit FamCommon(const eckit::URI& root);

    FamCommon(const Key& key, const Config& config);

    FamCommon(const eckit::URI& uri, const Config& config);

    virtual ~FamCommon() = default;

    // methods

    bool exists() const;

    eckit::URI uri() const;

    /// @note Throws if the region does not exist
    const eckit::FamRegion& getRegion() const;

    eckit::FamRegionName root_;

private:

    explicit FamCommon(const Config& config);

    mutable std::once_flag region_once_;
    mutable std::optional<eckit::FamRegion> region_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
