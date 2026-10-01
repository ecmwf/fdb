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

/// @file   S3Common.h
/// @author Metin Cakircali
/// @date   Dec 2024

#pragma once

#include "eckit/io/s3/S3BucketName.h"

namespace eckit {
class URI;
}

namespace fdb5 {

class Key;
class Config;

//----------------------------------------------------------------------------------------------------------------------

class S3Common {
public:  // methods

    S3Common(const Config& config);

    S3Common(const Config& config, const eckit::URI& uri);

    const eckit::S3BucketName& root() const { return root_; }

private:  // members

    eckit::S3BucketName root_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
