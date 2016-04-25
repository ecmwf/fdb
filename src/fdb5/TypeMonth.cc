/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/utils/Translator.h"
#include "eckit/types/Date.h"

#include "marslib/MarsTask.h"

#include "fdb5/TypesFactory.h"
#include "fdb5/TypeMonth.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeMonth::TypeMonth(const std::string& name, const std::string& type) :
    Type(name, type)
{
}

TypeMonth::~TypeMonth()
{
}

void TypeMonth::toKey(std::ostream& out,
                       const std::string& keyword,
                       const std::string& value) const {

    Date date(value);
    out << date.year() * 100 + date.month();
}

void TypeMonth::getValues(const MarsRequest& request,
                               const std::string& keyword,
                               StringList& values,
                               const MarsTask& task,
                               const DB* db) const
{
    std::vector<Date> dates;

    request.getValues(keyword, dates);

    values.reserve(dates.size());

    eckit::Translator<Date, std::string> t;

    for(std::vector<Date>::const_iterator i = dates.begin(); i != dates.end(); ++i) {
        const Date& date = *i;
        values.push_back(t(date));
    }
}

void TypeMonth::print(std::ostream &out) const
{
    out << "TypeMonth[name=" << name_ << "]";
}

static TypeBuilder<TypeMonth> type("Month");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
