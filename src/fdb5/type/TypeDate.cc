/*
 * (C) Copyright 1996-2016 ECMWF.
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

#include "fdb5/type/TypesFactory.h"
#include "fdb5/type/TypeDate.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeDate::TypeDate(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeDate::~TypeDate() {
}

void TypeDate::getValues(const MarsRequest &request,
                         const std::string &keyword,
                         eckit::StringList &values,
                         const MarsTask &task,
                         const DB *db) const {
    std::vector<eckit::Date> dates;

    request.getValues(keyword, dates);

    eckit::Translator<long, std::string> t;

    values.reserve(dates.size());

    for (std::vector<eckit::Date>::const_iterator i = dates.begin(); i != dates.end(); ++i) {
        values.push_back(t(i->yyyymmdd()));
    }
}

void TypeDate::print(std::ostream &out) const {
    out << "TypeDate[name=" << name_ << "]";
}

static TypeBuilder<TypeDate> type("Date");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
