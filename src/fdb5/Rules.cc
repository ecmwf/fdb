/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"

#include "fdb5/Rules.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Rules::Rules()
{
}

Rules::~Rules()
{
}

void Rules::print(std::ostream& out) const
{
    out << "Rules()";
}

std::ostream& operator<<(std::ostream& s, const Rules& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
