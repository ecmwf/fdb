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
#include "fdb5/AbbreviationHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

AbbreviationHandler::AbbreviationHandler(const std::string& name, const std::string& type) :
    KeywordHandler(name, type)
{
    count_ = char(type[type.length()-1]) - '0';
    std::cout << "AbbreviationHandler " << type << " - " << count_ << std::endl;
}

AbbreviationHandler::~AbbreviationHandler()
{
}

void AbbreviationHandler::getValues(const MarsRequest& request,
                               const std::string& keyword,
                               StringList& values,
                               const MarsTask& task,
                               const DB* db) const
{
    std::vector<std::string> vals;

    request.getValues(keyword, vals);

    values.reserve(vals.size());

    for(std::vector<std::string>::const_iterator i = vals.begin(); i != vals.end(); ++i) {
        std::cout << "AbbreviationHandler " << (*i) << " -> " << (*i).substr(0, count_) << std::endl;
        values.push_back((*i).substr(0, count_));
    }
}

void AbbreviationHandler::print(std::ostream &out) const
{
    out << "AbbreviationHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<AbbreviationHandler> handler("First3");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
