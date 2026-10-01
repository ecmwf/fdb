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

#include "fdb5/s3/S3Common.h"

#include "eckit/filesystem/URI.h"
#include "fdb5/s3/S3RootManager.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

S3Common::S3Common(const Config& config, const eckit::URI& uri) : root_{S3RootManager(config).root(uri)} {}

S3Common::S3Common(const Config& config) : S3Common(config, eckit::URI{}) {}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
