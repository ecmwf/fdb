/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include "eckit/exception/Exceptions.h"
#include "eckit/types/Date.h"
#include "eckit/utils/Translator.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/types/TypeYear.h"
#include "fdb5/types/TypesFactory.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeYear::TypeYear(const std::string& name, const std::string& type, const std::string& alias) :
    Type(name, type, alias) {}

TypeYear::~TypeYear() {}

std::string TypeYear::toKey(const std::string& value) const {

    eckit::Date date(value);
    return std::to_string(date.year());
}

void TypeYear::getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                         eckit::StringList& values, const Notifier&, const CatalogueReader*) const {
    std::vector<eckit::Date> dates;

    request.getValues(keyword, dates, true);

    values.reserve(dates.size());

    eckit::Translator<eckit::Date, std::string> t;

    for (const eckit::Date& date : dates) {
        values.emplace_back(t(date));
    }
}

void TypeYear::print(std::ostream& out) const {
    out << "TypeYear[name=" << name_ << "]";
}

static TypeBuilder<TypeYear> type("Year");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
