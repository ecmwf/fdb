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
#include "marslib/MarsRequest.h"

#include "fdb5/KeywordType.h"
#include "fdb5/DateHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DateHandler::DateHandler(const std::string& name) :
    KeywordHandler(name)
{
}

DateHandler::~DateHandler()
{
}

void DateHandler::getValues(const MarsRequest& request,
                               const std::string& keyword,
                               StringList& values,
                               const MarsTask& task,
                               const DB* db) const
{
    std::vector<Date> dates;

    request.getValues(keyword, dates);

    Translator<long, std::string> t;

    values.reserve(dates.size());

    for(std::vector<Date>::const_iterator i = dates.begin(); i != dates.end(); ++i) {
        values.push_back(t(i->yyyymmdd()));
    }
}

void DateHandler::print(std::ostream &out) const
{
    out << "DateHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<DateHandler> handler("Date");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
