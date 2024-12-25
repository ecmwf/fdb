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

#include "fdb5/s3/S3Root.h"

#include "eckit/config/LocalConfiguration.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

S3Root::S3Root(const eckit::LocalConfiguration& root)
    : endpoint_ {root.getString("endpoint")}, bucket_ {root.getString("bucket")} { }

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
