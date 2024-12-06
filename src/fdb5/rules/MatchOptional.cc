/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <string>
#include <utility>

#include "fdb5/rules/MatchOptional.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/Key.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MatchOptional::MatchOptional(std::string def): default_ {std::move(def)} { }

bool MatchOptional::match(const std::string& /*value*/) const {
    return true;
}

bool MatchOptional::match(const std::string&, const Key&) const {
    return true;
}

bool MatchOptional::optional() const {
    return true;
}

void MatchOptional::fill(Key& key, const std::string& keyword, const std::string& value) const {
    if (!value.empty()) {
        key.push(keyword, value);
    }
}

const std::string& MatchOptional::value(const Key& key, const std::string& keyword) const {

    if (const auto [iter, found] = key.find(keyword); found) { return iter->second; }

    return default_[0];
}

const std::vector<std::string>& MatchOptional::values(const metkit::mars::MarsRequest& rq, const std::string& keyword) const {

    if (rq.has(keyword)) {
        return rq.values(keyword);
    }
    return default_;
}

const std::string &MatchOptional::defaultValue() const {
    return default_[0];
}

void MatchOptional::dump(std::ostream &s, const std::string &keyword, const TypesRegistry &registry) const {
    registry.dump(s, keyword);
    s << '?' << default_[0];
}

void MatchOptional::print(std::ostream &out) const {
    out << "MatchOptional[default=" << default_[0] << "]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
