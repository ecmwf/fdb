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

/// @file   S3RootManager.h
/// @author Metin Cakircali
/// @date   Dec 2024

#include "eckit/config/LocalConfiguration.h"
#include "eckit/io/s3/S3BucketName.h"
#include "fdb5/config/Config.h"

#include <vector>

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class S3RootManager {
public:  // methods
    explicit S3RootManager(const Config& config);

    // rules
    S3RootManager(const S3RootManager&)            = default;
    S3RootManager& operator=(const S3RootManager&) = default;

    S3RootManager(S3RootManager&&)            = delete;
    S3RootManager& operator=(S3RootManager&&) = delete;

    virtual ~S3RootManager() = default;

    /// Uniquely selects a root using the databaseKey
    eckit::S3BucketName root(const Key& databaseKey) const;

protected:  // methods
    virtual std::vector<eckit::S3BucketName> parseRoots() const;

private:  // members
    eckit::LocalConfiguration config_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
