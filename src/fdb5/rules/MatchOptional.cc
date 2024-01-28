/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/rules/MatchOptional.h"

#include "eckit/log/Log.h"
#include "eckit/types/Types.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/Key.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

static std::string empty;

//----------------------------------------------------------------------------------------------------------------------

MatchOptional::MatchOptional(const std::string &def) :
    Matcher() {
    default_.push_back(def);
}

MatchOptional::~MatchOptional() {
}

bool MatchOptional::match(const std::string&, const Key&) const {
    return true;
}

bool MatchOptional::match(const std::string&) const {
    return true;
}

bool MatchOptional::optional() const {
    return true;
}

void MatchOptional::fill(Key &key, const std::string &keyword, const std::string& value) const {
    if (!value.empty()) {
        key.push(keyword, value);
    }
}

const std::string &MatchOptional::value(const Key &key, const std::string &keyword) const {
    Key::const_iterator i = key.find(keyword);

    if (i == key.end()) {
        return default_[0];
    }

    return key.get(keyword);
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
