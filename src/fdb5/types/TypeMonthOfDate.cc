// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// #include "eckit/exception/Exceptions.h"
#include <iomanip>
#include <sstream>

#include "eckit/types/Date.h"
#include "eckit/utils/Translator.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/types/TypeMonthOfDate.h"
#include "fdb5/types/TypesFactory.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeMonthOfDate::TypeMonthOfDate(const std::string& name, const std::string& type, const std::string& alias) :
    Type(name, type, alias) {}

TypeMonthOfDate::~TypeMonthOfDate() {}

std::string TypeMonthOfDate::toKey(const std::string& value) const {

    eckit::Date date(value);

    std::ostringstream ss;
    ss << std::setw(2) << std::setfill('0') << date.month();
    return ss.str();
}

void TypeMonthOfDate::getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                                eckit::StringList& values, const CatalogueReader*) const {
    std::vector<eckit::Date> dates;

    request.getValues(keyword, dates, true);

    values.reserve(dates.size());

    eckit::Translator<eckit::Date, std::string> t;

    for (std::vector<eckit::Date>::const_iterator i = dates.begin(); i != dates.end(); ++i) {
        const eckit::Date& date = *i;
        values.push_back(t(date));
    }
}

void TypeMonthOfDate::print(std::ostream& out) const {
    out << "TypeMonthOfDate[name=" << name_ << "]";
}

static TypeBuilder<TypeMonthOfDate> type("MonthOfDate");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
