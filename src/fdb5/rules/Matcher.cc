// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/exception/Exceptions.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/Key.h"
#include "fdb5/rules/Matcher.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec Matcher::classSpec_ = {&eckit::Streamable::classSpec(), "Matcher"};

Matcher::Matcher(eckit::Stream& stream) {}

void Matcher::encode(eckit::Stream& out) const {}

const std::string& Matcher::value(const Key& key, const std::string& keyword) const {
    return key.get(keyword);
}

const std::vector<std::string>& Matcher::values(const metkit::mars::MarsRequest& rq, const std::string& keyword) const {
    return rq.values(keyword);
}

void Matcher::fill(Key& key, const std::string& keyword, const std::string& value) const {
    key.push(keyword, value);
}

const std::vector<std::string>& Matcher::optionalValues() const {
    NOTIMP;
}

const std::string& Matcher::defaultValue() const {
    NOTIMP;
}

std::ostream& operator<<(std::ostream& s, const Matcher& x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
