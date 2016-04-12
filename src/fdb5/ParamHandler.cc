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
#include "eckit/types/Types.h"
#include "eckit/log/Log.h"
#include "eckit/utils/Translator.h"

#include "fdb5/KeywordType.h"
#include "fdb5/ParamHandler.h"
#include "fdb5/DB.h"

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

void ParamHandler::getValues(const MarsRequest& request,
                             const std::string& keyword,
                             StringList& values) const {

    return KeywordHandler::getValues(request, keyword, values);
    // StringSet axis;

    // db.axis(key, keyword, axis);

    // Log::info() << "Axis set ";
    // eckit::__print_container(Log::info(),axis);
    // Log::info() << std::endl;

    // StringList tmp;

    // KeywordHandler::getValues(task, keyword, tmp, db, key);
    // Translator<std::string, int> t;
    // for(StringList::const_iterator i = tmp.begin(); i != tmp.end(); ++i) {
    //     if(axis.find(*i) != axis.end()) {
    //         values.push_back(*i);
    //     }
    //     else {
    //         bool found = false;
    //         for(StringSet::const_iterator j = axis.begin(); j != axis.end(); ++j) {

    //             // This assumes that the user only provided the 3 last digits of paramId
    //             // e.g. backward compatibility with wave paramId = 140229 but user gives 229 only

    //             if( t(*j) % 1000 == t(*i) ) {
    //                 found = true;
    //                 values.push_back(*j);
    //                 break;
    //             }
    //         }
    //         if(!found) {
    //             values.push_back(*i); /// @note we know this will fail, but server will print meaningful message
    //         }
    //     }
    // }

    // Log::info() << "ParamHandler returns values: " << values << std::endl;
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
