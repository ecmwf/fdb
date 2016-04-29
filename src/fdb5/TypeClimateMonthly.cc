/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "eckit/types/Date.h"
#include "marslib/MarsRequest.h"

#include "fdb5/TypesFactory.h"
#include "fdb5/TypeClimateMonthly.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static const char *months[] = {
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
};

TypeClimateMonthly::TypeClimateMonthly(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeClimateMonthly::~TypeClimateMonthly() {
}

static int month(const std::string &value) {
    if (isdigit(value[0])) {
        eckit::Date date(value);
        return date.month();
    } else {

        for (int i = 0; i < 12 ; i++ ) {
            if (value == months[i]) {
                return i + 1;
            }
        }

        throw eckit::SeriousBug("TypeClimateMonthly: invalid date: " + value);
    }
}

void TypeClimateMonthly::toKey(std::ostream &out,
                               const std::string &keyword,
                               const std::string &value) const {

    out << month(value);
}

void TypeClimateMonthly::getValues(const MarsRequest &request,
                                   const std::string &keyword,
                                   eckit::StringList &values,
                                   const MarsTask &task,
                                   const DB *db) const {
    std::vector<std::string> dates;

    request.getValues(keyword, dates);

    values.reserve(dates.size());

    for (std::vector<std::string>::const_iterator i = dates.begin(); i != dates.end(); ++i) {
        values.push_back(months[month(*i) - 1]);
    }
}

void TypeClimateMonthly::print(std::ostream &out) const {
    out << "TypeClimateMonthly[name=" << name_ << "]";
}

static TypeBuilder<TypeClimateMonthly> type("ClimateMonthly");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
