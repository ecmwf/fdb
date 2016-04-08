/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sstream>

#include "fdb5/NotFound.h"
#include "fdb5/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

NotFound::NotFound(const Key& r, const eckit::CodeLocation& loc)
{
    std::ostringstream oss;

    oss << "FDB5 not found: " << r;

    reason( oss.str() );
}

NotFound::~NotFound() throw ()
{
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
