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

#include "fdb5/ParamKeywordHandler.h"

#include "fdb5/UVOp.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ParamKeywordHandler::ParamKeywordHandler(const std::string& name) :
    KeywordHandler(name)
{
}

ParamKeywordHandler::~ParamKeywordHandler()
{
}

Op* ParamKeywordHandler::makeOp(const MarsTask& task, Op& parent) const {
    return new UVOp(task, parent);
}

void ParamKeywordHandler::print(std::ostream &out) const
{
    out << "ParamKeywordHandler(" << name_ << ")";
}

static ParamKeywordHandler handler("param");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
