/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/utils/Translator.h"

#include "eckit/types/Date.h"
#include "marslib/MarsRequest.h"

#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/TypeDate.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeDate::TypeDate(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeDate::~TypeDate() {
}


std::string TypeDate::tidy(
                     const std::string&,
                     const std::string &value) const {
    if (!value.empty() && (value[0] == '0' || value[0] == '-')) {
        eckit::Translator<std::string, long> t;
        long n = t(value);
        if (n <= 0) {
            eckit::Date now(n);
            eckit::Translator<long, std::string> t;
            return t(now.yyyymmdd());
        }
    }
    return value;

}

void TypeDate::getValues(const MarsRequest & request,
                         const std::string & keyword,
                         eckit::StringList & values,
                         const NotifyWind&,
                         const DB*) const {
    std::vector<eckit::Date> dates;

    request.getValues(keyword, dates);

    eckit::Translator<long, std::string> t;

    values.reserve(dates.size());

    for (std::vector<eckit::Date>::const_iterator i = dates.begin(); i != dates.end(); ++i) {
        values.push_back(t(i->yyyymmdd()));
    }
}

void TypeDate::print(std::ostream & out) const {
    out << "TypeDate[name=" << name_ << "]";
}

static TypeBuilder<TypeDate> type("Date");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
