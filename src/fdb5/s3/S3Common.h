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

#include "eckit/io/s3/S3BucketName.h"

namespace fdb5 {

class Key;
class Config;

//----------------------------------------------------------------------------------------------------------------------

struct S3Common {
    S3Common(const Key& databaseKey, const Config& config);

    eckit::S3BucketName root_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
