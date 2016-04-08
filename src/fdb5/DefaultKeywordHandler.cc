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

#include "fdb5/KeywordType.h"
#include "fdb5/DefaultKeywordHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DefaultKeywordHandler::DefaultKeywordHandler(const std::string& name) :
    KeywordHandler(name)
{
}

DefaultKeywordHandler::~DefaultKeywordHandler()
{
}

void DefaultKeywordHandler::print(std::ostream &out) const
{
    out << "DefaultKeywordHandler()";
}

static KeywordHandlerBuilder<DefaultKeywordHandler> handler("Default");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
