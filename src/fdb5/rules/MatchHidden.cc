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
#include <string>
#include <utility>

#include "fdb5/database/Key.h"
#include "fdb5/rules/MatchHidden.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec MatchHidden::classSpec_ = {
    &Matcher::classSpec(),
    "MatchHidden",
};

eckit::Reanimator<MatchHidden> MatchHidden::reanimator_;


MatchHidden::MatchHidden(std::string def) : default_{std::move(def)} {}

MatchHidden::MatchHidden(eckit::Stream& stream) : Matcher() {

    size_t numValues;
    std::string value;

    stream >> numValues;
    for (size_t i = 0; i < numValues; i++) {
        stream >> value;
        default_.push_back(value);
    }
}

void MatchHidden::encode(eckit::Stream& s) const {
    s << default_.size();
    for (const std::string& value : default_) {
        s << value;
    }
}

const std::string& MatchHidden::value(const Key&, const std::string&) const {
    return default_[0];
}

const std::vector<std::string>& MatchHidden::values(const metkit::mars::MarsRequest& rq,
                                                    const std::string& keyword) const {
    return default_;
}

const std::string& MatchHidden::defaultValue() const {
    return default_[0];
}

void MatchHidden::dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const {
    registry.dump(s, keyword);
    s << '-' << default_[0];
}

void MatchHidden::print(std::ostream& out) const {
    out << "MatchHidden[default" << default_[0] << "]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
