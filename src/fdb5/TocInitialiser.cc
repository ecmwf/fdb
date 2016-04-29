/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <fcntl.h>

#include "eckit/io/FileHandle.h"

#include "fdb5/TocInitialiser.h"

#include "fdb5/MasterConfig.h"


namespace fdb5 {

//-----------------------------------------------------------------------------

TocInitialiser::TocInitialiser(const eckit::PathName &dir, const Key& tocKey) : TocHandler(dir) {

}

//-----------------------------------------------------------------------------

} // namespace fdb5
