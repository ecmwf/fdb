/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Key.h"
#include "fdb5/rules/MatchValue.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MatchValue::MatchValue(const std::string &value) :
    Matcher(),
    value_(value) {
}

MatchValue::~MatchValue() {
}

bool MatchValue::match(const std::string &keyword, const Key &key) const {
    Key::const_iterator i = key.find(keyword);

    if (i == key.end()) {
        return false;
    }
    return MatchValue::match(i->second);
}

bool MatchValue::match(const std::string& value) const {
    return value == value_;
}

void MatchValue::dump(std::ostream &s, const std::string &keyword, const TypesRegistry &registry) const {
    registry.dump(s, keyword);
    s << "=" << value_;
}

void MatchValue::print(std::ostream &out) const {
    out << "MatchValue[value=" << value_ << "]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
