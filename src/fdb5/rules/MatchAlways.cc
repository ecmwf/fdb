/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <ostream>

#include "fdb5/rules/MatchAlways.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec MatchAlways::classSpec_ = { &Matcher::classSpec(), "MatchAlways", };

eckit::Reanimator<MatchAlways> MatchAlways::reanimator_;


MatchAlways::MatchAlways(eckit::Stream&) :
    Matcher() {
}

void MatchAlways::encode(eckit::Stream& s) const {
}

void MatchAlways::dump(std::ostream &s, const std::string &keyword, const TypesRegistry &registry) const {
    registry.dump(s, keyword);
}

void MatchAlways::print(std::ostream &out) const {
    out << "MatchAlways[]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
