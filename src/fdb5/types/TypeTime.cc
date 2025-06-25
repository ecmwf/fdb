/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/types/TypeTime.h"

#include <iomanip>

#include "eckit/types/Date.h"
#include "eckit/utils/Translator.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/types/TypesFactory.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeTime::TypeTime(const std::string& name, const std::string& type) : Type(name, type) {}

TypeTime::~TypeTime() {}

std::string TypeTime::tidy(const std::string&, const std::string& value) const {

    eckit::Translator<std::string, long> t;

    long n = t(value);
    if (n < 100) {
        n *= 100;
    }

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << n;
    return oss.str();
}

std::string TypeTime::toKey(const std::string& keyword, const std::string& value) const {

    // if value just contains a digit, add a leading zero to be compliant with eckit::Time
    std::string t = value.size() < 2 ? "0" + value : value;
    eckit::Time time(t);

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(2) << time.hours() << std::setfill('0') << std::setw(2) << time.minutes();
    return oss.str();
}

void TypeTime::print(std::ostream& out) const {
    out << "TypeTime[name=" << name_ << "]";
}

static TypeBuilder<TypeTime> type("Time");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
