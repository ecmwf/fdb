/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/utils/Translator.h"
#include "eckit/utils/Tokenizer.h"
#include "eckit/types/Date.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/TypeClimateDaily.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static const char *months[] = {
  "jan", "feb", "mar", "apr", "may", "jun",
  "jul", "aug", "sep", "oct", "nov", "dec",
};

TypeClimateDaily::TypeClimateDaily(const std::string &name, const std::string &type) :
  Type(name, type) {
}

TypeClimateDaily::~TypeClimateDaily() {
}

static int month(const std::string &value) {
  if (isdigit(value[0])) {
    eckit::Date date(value);
    return date.month() * 100 + date.day();
  } else {

    eckit::Translator<std::string, long> t;
    eckit::Tokenizer parse("-");
    eckit::StringList v;

    parse(value, v);
    ASSERT(v.size() == 2);



    for (int i = 0; i < 12 ; i++ ) {
      if (v[0] == months[i]) {
        return (i + 1) * 100 + t(v[1]);
      }
    }

    throw eckit::SeriousBug("TypeClimateDaily: invalid date: " + value);
  }
}

std::string TypeClimateDaily::toKey(const std::string&,
                                    const std::string &value) const {
  std::ostringstream out;
  char prev = out.fill ('0');
  out.width(4);
  out << month(value);
  out.fill(prev);
  return out.str();
}

void TypeClimateDaily::getValues(const metkit::mars::MarsRequest &request,
                                 const std::string &keyword,
                                 eckit::StringList &values,
                                 const Notifier&,
                                 const CatalogueReader*) const {
  std::vector<eckit::Date> dates;

  request.getValues(keyword, dates, true);

  values.reserve(dates.size());

  eckit::Translator<eckit::Date, std::string> t;

  for (std::vector<eckit::Date>::const_iterator i = dates.begin(); i != dates.end(); ++i) {
    values.push_back(t(*i));
  }
}

bool TypeClimateDaily::match(const std::string&,
                   const std::string& value1,
                   const std::string& value2) const {
    // std::cout << value1 << " vs. " << value2 << " are " << (month(value1) == month(value2)) << std::endl;

    return month(value1) == month(value2);
}

void TypeClimateDaily::print(std::ostream &out) const {
  out << "TypeClimateDaily[name=" << name_ << "]";
}

static TypeBuilder<TypeClimateDaily> type("ClimateDaily");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
