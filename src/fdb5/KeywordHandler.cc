/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "marslib/MarsTask.h"

#include "fdb5/KeywordHandler.h"

#include "fdb5/Op.h"
#include "fdb5/ForwardOp.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

KeywordHandler::KeywordHandler(const std::string& name) :
    name_(name)
{
}

KeywordHandler::~KeywordHandler() {
}

void KeywordHandler::getValues(const MarsTask& task,
                               const std::string& keyword,
                               StringList& values,
                               const DB& db,
                               const Key& key) const
{
    task.request().getValues(keyword, values);
}

Op*KeywordHandler::makeOp(const MarsTask& task, Op& parent) const {
    return new ForwardOp(parent);
}

std::ostream& operator<<(std::ostream& s, const KeywordHandler& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
