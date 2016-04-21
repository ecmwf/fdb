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
#include "eckit/parser/StringTools.h"
#include "marslib/MarsRequest.h"

#include "fdb5/KeywordType.h"
#include "fdb5/GridHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

GridHandler::GridHandler(const std::string& name) :
    KeywordHandler(name)
{
}

GridHandler::~GridHandler()
{
}

void GridHandler::getValues(const MarsRequest& request,
                               const std::string& keyword,
                               StringList& values,
                               const MarsTask& task,
                               const DB* db) const
{
    std::vector<std::string> v;
    request.getValues(keyword, v);
    values.push_back(StringTools::join("@", v));
}


void GridHandler::print(std::ostream &out) const
{
    out << "GridHandler()";
}

static KeywordHandlerBuilder<GridHandler> handler("Grid");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
