/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   Dec 2023

#pragma once

#include "eckit/system/Plugin.h"

namespace fdb5 {
namespace httpsvr {

//----------------------------------------------------------------------------------------------------------------------

class LibHttpSvrFdb5 : public eckit::system::Plugin {

public:  // methods
    LibHttpSvrFdb5();

    static LibHttpSvrFdb5& instance();

protected:  // methods
    const void* addr() const override;

    std::string version() const override;

    std::string gitsha1(unsigned int count) const override;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace httpsvr
} // namespace fdb5
