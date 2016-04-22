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
#include "fdb5/ClimateHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static const char* months[] = {
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",};

ClimateHandler::ClimateHandler(const std::string& name) :
    KeywordHandler(name)
{
}

ClimateHandler::~ClimateHandler()
{
}

static int month(const std::string& value) {
  if(isdigit(value[0])) {
      Date date(value);
      return date.month();
  }
  else {

      for(int i = 0; i < 12 ; i++ ) {
          if(value == months[i]) {
              return i+1;
          }
      }

      throw SeriousBug("ClimateHandler: invalid date: " + value);
  }
}

void ClimateHandler::toKey(std::ostream& out,
                       const std::string& keyword,
                       const std::string& value) const {

    out << month(value);
}

void ClimateHandler::getValues(const MarsRequest& request,
                               const std::string& keyword,
                               StringList& values,
                               const MarsTask& task,
                               const DB* db) const
{
    std::vector<std::string> dates;

    request.getValues(keyword, dates);

    values.reserve(dates.size());

    eckit::Translator<long, std::string> t;

    for(std::vector<std::string>::const_iterator i = dates.begin(); i != dates.end(); ++i) {
        values.push_back(t(month(*i)));
    }
}

void ClimateHandler::print(std::ostream &out) const
{
    out << "ClimateHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<ClimateHandler> handler("Climate");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
