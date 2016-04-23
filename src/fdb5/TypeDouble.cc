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

#include "marslib/MarsTask.h"

#include "fdb5/TypesFactory.h"
#include "fdb5/TypeDouble.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeDouble::TypeDouble(const std::string& name, const std::string& type) :
    Type(name, type)
{
}

TypeDouble::~TypeDouble()
{
}

void TypeDouble::toKey(std::ostream& out,
                       const std::string& keyword,
                       const std::string& value) const {
    out << eckit::Translator<std::string, double>()(value);
}

void TypeDouble::getValues(const MarsRequest& request,
                               const std::string& keyword,
                               StringList& values,
                               const MarsTask& task,
                               const DB* db) const
{
    std::vector<double> dblValues;

    request.getValues(keyword, dblValues);

    Translator<double, std::string> t;

    values.reserve(dblValues.size());

    for(std::vector<double>::const_iterator i = dblValues.begin(); i != dblValues.end(); ++i) {
        values.push_back(t(*i));
    }
}

void TypeDouble::print(std::ostream &out) const
{
    out << "TypeDouble(" << name_ << ")";
}

static TypeBuilder<TypeDouble> type("Double");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
