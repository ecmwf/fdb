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

#include "fdb5/KeywordType.h"
#include "fdb5/DoubleHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DoubleHandler::DoubleHandler(const std::string& name, const std::string& type) :
    KeywordHandler(name, type)
{
}

DoubleHandler::~DoubleHandler()
{
}

void DoubleHandler::toKey(std::ostream& out,
                       const std::string& keyword,
                       const std::string& value) const {
    out << eckit::Translator<std::string, double>()(value);
}

void DoubleHandler::getValues(const MarsRequest& request,
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

void DoubleHandler::print(std::ostream &out) const
{
    out << "DoubleHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<DoubleHandler> handler("Double");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
