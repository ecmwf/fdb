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

#include "fdb5/database/Key.h"
#include "fdb5/rules/MatchAny.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec MatchAny::classSpec_ = {
    &Matcher::classSpec(),
    "MatchAny",
};

eckit::Reanimator<MatchAny> MatchAny::reanimator_;


MatchAny::MatchAny(const std::set<std::string>& values) : Matcher(), values_(values) {}

MatchAny::MatchAny(eckit::Stream& s) : Matcher() {

    size_t numValues;
    std::string value;

    s >> numValues;
    for (size_t i = 0; i < numValues; i++) {
        s >> value;
        values_.insert(value);
    }
}

void MatchAny::encode(eckit::Stream& s) const {
    s << values_.size();
    for (const std::string& value : values_) {
        s << value;
    }
}

bool MatchAny::match(const std::string& keyword, const Key& key) const {

    if (const auto [iter, found] = key.find(keyword); found) {
        return match(iter->second);
    }

    return false;
}

bool MatchAny::match(const std::string& value) const {
    return (values_.find(value) != values_.end());
}

void MatchAny::dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const {
    const char* sep = "";
    registry.dump(s, keyword);
    s << "=";
    for (std::set<std::string>::const_iterator i = values_.begin(); i != values_.end(); ++i) {
        s << sep << *i;
        sep = "/";
    }
}

void MatchAny::print(std::ostream& out) const {
    out << "MatchAny[values=";
    const char* sep = "";
    for (std::set<std::string>::const_iterator i = values_.begin(); i != values_.end(); ++i) {
        out << sep << *i;
        sep = ",";
    }
    out << "]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
