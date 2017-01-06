/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <ostream>

#include "fdb5/pmem/PMemEngine.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::string PMemEngine::name() const {
    return PMemEngine::typeName();
}

std::string PMemEngine::dbType() const {
    return PMemEngine::typeName();
}

void PMemEngine::print(std::ostream& out) const
{
    out << "PMemEngine()";
}

static EngineBuilder<PMemEngine> builder(PMemEngine::typeName());

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
