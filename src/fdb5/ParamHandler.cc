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
#include "fdb5/ParamHandler.h"

#include "fdb5/UVOp.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ParamHandler::ParamHandler(const std::string& name) :
    KeywordHandler(name)
{
}

ParamHandler::~ParamHandler()
{
}

Op* ParamHandler::makeOp(const MarsTask& task, Op& parent) const {
    return new UVOp(task, parent);
}

void ParamHandler::print(std::ostream &out) const
{
    out << "ParamHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<ParamHandler> handler("Param");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
