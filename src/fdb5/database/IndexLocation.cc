// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/database/IndexLocation.h"

using namespace eckit;

namespace fdb5 {

::eckit::ClassSpec IndexLocation::classSpec_ = {
    &Streamable::classSpec(),
    "IndexLocation",
};

//----------------------------------------------------------------------------------------------------------------------

IndexLocation::IndexLocation() {}

IndexLocationVisitor::~IndexLocationVisitor() {}

IndexLocation::~IndexLocation() {}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
