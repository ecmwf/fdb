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

/// @file   FamRecordVersion.h
/// @author Metin Cakircali
/// @date   Jul 2024

#pragma once

#include <array>
#include <string>

namespace fdb5 {

class Config;

//----------------------------------------------------------------------------------------------------------------------

class FamRecordVersion {
public:  // types
    using value_type = unsigned char;

public:  // statics
    static constexpr auto supported = std::array<value_type, 1> {1};

    static constexpr auto latest = value_type {1};

    static constexpr auto defaulted = value_type {1};

    static auto supportedStr() -> std::string;

    /// @throw eckit::BadValue on failure
    static void check(value_type version);

public:  // methods
    FamRecordVersion(const Config& config);

    ~FamRecordVersion() = default;

    auto operator*() const -> const value_type& { return value_; }

    auto value() const -> value_type { return value_; }

private:  // members
    const value_type value_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
