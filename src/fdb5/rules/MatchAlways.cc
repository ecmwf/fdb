// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/rules/MatchAlways.h"

#include <ostream>
#include <string>

#include "eckit/serialisation/Reanimator.h"
#include "eckit/serialisation/Stream.h"

#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec MatchAlways::classSpec_ = {
    &Matcher::classSpec(),
    "MatchAlways",
};

eckit::Reanimator<MatchAlways> MatchAlways::reanimator_;


MatchAlways::MatchAlways(eckit::Stream&) : Matcher() {}

void MatchAlways::encode(eckit::Stream& s) const {}

void MatchAlways::dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const {
    registry.dump(s, keyword);
}

void MatchAlways::print(std::ostream& out) const {
    out << "MatchAlways[]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
