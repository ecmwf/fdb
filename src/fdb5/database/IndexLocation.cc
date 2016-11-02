/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"

#include "fdb5/database/IndexLocation.h"
#include "fdb5/toc/TocIndexLocation.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


/// These are base methods (with default behaviour) for a visitor-pattern dispatch

void IndexLocationVisitor::operator() (const IndexLocation& location) {

    throw SeriousBug("Should never hit the default case visitor (base IndexLocation)", Here());

}


void IndexLocationVisitor::operator() (const TocIndexLocation& location) {

    throw SeriousBug("Should never hit the default case visitor (TocIndexLocation)", Here());

}

//----------------------------------------------------------------------------------------------------------------------

}
