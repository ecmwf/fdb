// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/utils/Translator.h"

#include "eckit/types/Date.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/types/TypeDate.h"
#include "fdb5/types/TypesFactory.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeDate::TypeDate(const std::string& name, const std::string& type) : Type(name, type) {}

TypeDate::~TypeDate() {}


std::string TypeDate::tidy(const std::string& value) const {

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

void TypeDate::getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                         eckit::StringList& values, const CatalogueReader*) const {
    std::vector<eckit::Date> dates;

    request.getValues(keyword, dates, true);

    eckit::Translator<long, std::string> t;

    values.reserve(dates.size());

    for (std::vector<eckit::Date>::const_iterator i = dates.begin(); i != dates.end(); ++i) {
        values.push_back(t(i->yyyymmdd()));
    }
}

void TypeDate::print(std::ostream& out) const {
    out << "TypeDate[name=" << name_ << "]";
}

static TypeBuilder<TypeDate> type("Date");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
