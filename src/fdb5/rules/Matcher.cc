/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/rules/Matcher.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Matcher::Matcher() {
}

Matcher::~Matcher() {
}

bool Matcher::optional() const {
    return false;
}

const std::string &Matcher::value(const Key& key, const std::string &keyword) const {
    return key.get(keyword);
}

const std::vector<std::string> &Matcher::values(const metkit::mars::MarsRequest& rq, const std::string &keyword) const {
    return rq.values(keyword);
}

void Matcher::fill(BaseKey& key, const std::string &keyword, const std::string& value) const {
    key.push(keyword, value);
}


const std::string &Matcher::defaultValue() const {
    NOTIMP;
}

std::ostream &operator<<(std::ostream &s, const Matcher &x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
