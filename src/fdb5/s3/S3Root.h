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
 * This software was developed as part of the Horizon Europe programme funded project DaFab
 * (Grant agreement: 101128693) https://www.dafab-ai.eu/
 */

/// @file   S3Config.h
/// @author Metin Cakircali
/// @date   Dec 2024

#pragma once

#include "eckit/config/LocalConfiguration.h"
#include "eckit/net/Endpoint.h"

#include <string>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

struct S3Root {

    explicit S3Root(const eckit::LocalConfiguration& root);

    eckit::net::Endpoint endpoint_;
    std::string          bucket_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
