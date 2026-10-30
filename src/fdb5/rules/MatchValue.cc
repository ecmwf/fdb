// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <ostream>
#include <string>
#include <utility>

#include "fdb5/database/Key.h"
#include "fdb5/rules/MatchValue.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec MatchValue::classSpec_ = {&Matcher::classSpec(), "MatchValue"};

eckit::Reanimator<MatchValue> MatchValue::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

MatchValue::MatchValue(std::string value) : value_{std::move(value)} {}

MatchValue::MatchValue(eckit::Stream& stream) {
    stream >> value_;
}

void MatchValue::encode(eckit::Stream& out) const {
    out << value_;
}

bool MatchValue::match(const std::string& value) const {
    return value == value_;
}

bool MatchValue::match(const std::string& keyword, const Key& key) const {

    if (const auto [iter, found] = key.find(keyword); found) {
        return match(iter->second);
    }

    return false;
}

void MatchValue::dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const {
    registry.dump(s, keyword);
    s << "=" << value_;
}

void MatchValue::print(std::ostream& out) const {
    out << "MatchValue[value=" << value_ << "]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
