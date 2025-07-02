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

#include "eckit/io/fam/FamPath.h"
#include "eckit/io/fam/FamRegionName.h"

namespace fdb5 {

class Key;
class Config;

//----------------------------------------------------------------------------------------------------------------------

struct FamCommon {
    static constexpr auto type = eckit::FamPath::scheme;

    static auto toString(const Key& key) -> std::string;

    FamCommon(const eckit::FamRegionName& root);

    FamCommon(const Config& config);

    FamCommon(const Config& config, const Key& key);

    virtual ~FamCommon() = default;

    auto exists() const -> bool;

    auto uri() const -> eckit::URI;

    eckit::FamRegionName root_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
