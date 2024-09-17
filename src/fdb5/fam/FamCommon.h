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

#include "eckit/io/fam/FamRegionName.h"

namespace fdb5 {

class Key;
class Config;

//----------------------------------------------------------------------------------------------------------------------

class FamCommon {
public:  // types
    static constexpr const char* TYPE = "fam";

public:  // methods
    FamCommon(const Config& config, const Key& key);

    virtual ~FamCommon();

protected:  // methods
    static auto toString(const Key& key) -> std::string;

    auto root() const -> const eckit::FamRegionName& { return root_; }

private:  // members
    const eckit::FamRegionName root_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
