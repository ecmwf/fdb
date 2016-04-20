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
#include "marslib/StepRange.h"

#include "fdb5/KeywordType.h"
#include "fdb5/StepHandler.h"
#include "fdb5/DB.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

StepHandler::StepHandler(const std::string& name) :
    KeywordHandler(name)
{
}

StepHandler::~StepHandler()
{
}

void StepHandler::getValues(const MarsRequest& request,
                               const std::string& keyword,
                               StringList& values,
                               const MarsTask& task,
                               const DB* db) const
{
    std::vector<std::string> steps;

    request.getValues(keyword, steps);

    Translator<StepRange, std::string> t;

    values.reserve(steps.size());

    for(std::vector<std::string>::const_iterator i = steps.begin(); i != steps.end(); ++i) {
        values.push_back(t(StepRange(*i)));
    }

    if(db) { 
        // We can only do that if step is at the index level

        StringSet axis;
        StringList newvalues;

        db->axis(keyword, axis);

        for(std::vector<std::string>::const_iterator i = values.begin(); i != values.end(); ++i) {
            const std::string& step = *i;

            if(axis.find(step) == axis.end()) {
                std::string tmp = std::string("0-") + step;
                 if(axis.find(tmp) != axis.end()) {
                    newvalues.push_back(tmp);
                 }
                 else {
                     tmp = step + "-" + step;
                     if(axis.find(tmp) != axis.end()) {
                        newvalues.push_back(tmp);
                     }
                     else {
                        newvalues.push_back(step);
                     }
                 }
            }
            else {
                newvalues.push_back(step);
            }
        }

        Log::info() << "StepHandler before: " << values << std::endl;
        Log::info() << "              after: " << newvalues << std::endl;
        Log::info() << "               axis: " << axis << std::endl;

        std::swap(values, newvalues);

    }
}

void StepHandler::print(std::ostream &out) const
{
    out << "StepHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<StepHandler> handler("Step");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
