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

#include "fdb5/KeywordType.h"
#include "fdb5/MonthHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MonthHandler::MonthHandler(const std::string& name) :
    KeywordHandler(name)
{
}

MonthHandler::~MonthHandler()
{
}

void MonthHandler::toKey(std::ostream& out,
                       const std::string& keyword,
                       const std::string& value) const {
  Date date(value);
  out << date.year() * 1000 + date.month();
}

void MonthHandler::getValues(const MarsRequest& request,
                               const std::string& keyword,
                               StringList& values,
                               const MarsTask& task,
                               const DB* db) const
{
    std::vector<Date> dates;

    request.getValues(keyword, dates);

    values.reserve(dates.size());

    eckit::Translator<long, std::string> t;

    for(std::vector<Date>::const_iterator i = dates.begin(); i != dates.end(); ++i) {
        const Date& date = *i;
        values.push_back(t(date.year() * 1000 + date.month()));
    }
}

void MonthHandler::print(std::ostream &out) const
{
    out << "MonthHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<MonthHandler> handler("Month");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
