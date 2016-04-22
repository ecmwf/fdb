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
#include "fdb5/IntegerHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

IntegerHandler::IntegerHandler(const std::string& name, const std::string& type) :
    KeywordHandler(name, type)
{
}

IntegerHandler::~IntegerHandler()
{
}

void IntegerHandler::getValues(const MarsRequest& request,
                               const std::string& keyword,
                               StringList& values,
                               const MarsTask& task,
                               const DB* db) const
{
    std::vector<long> intValues;

    request.getValues(keyword, intValues);

    Translator<long, std::string> t;

    values.reserve(intValues.size());

    for(std::vector<long>::const_iterator i = intValues.begin(); i != intValues.end(); ++i) {
        values.push_back(t(*i));
    }
}

void IntegerHandler::print(std::ostream &out) const
{
    out << "IntegerHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<IntegerHandler> handler("Integer");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
