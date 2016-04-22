/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/utils/Translator.h"

#include "marslib/MarsTask.h"
#include "marslib/StepRange.h"

#include "fdb5/KeywordType.h"
#include "fdb5/IgnoreHandler.h"
#include "fdb5/DB.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

IgnoreHandler::IgnoreHandler(const std::string &name) :
    KeywordHandler(name) {
}

IgnoreHandler::~IgnoreHandler() {
}


void IgnoreHandler::toKey(std::ostream& out,
                       const std::string& keyword,
                       const std::string& value) const {
  out << StepRange(value);
}

void IgnoreHandler::getValues(const MarsRequest &request,
                            const std::string &keyword,
                            eckit::StringList &values,
                            const MarsTask &task,
                            const DB *db) const {
    std::vector<std::string> steps;

    request.getValues(keyword, steps);

    eckit::Translator<StepRange, std::string> t;

    values.reserve(steps.size());

    if(db && !steps.empty()) {
        StringSet axis;
        db->axis(keyword, axis);
        for (std::vector<std::string>::const_iterator i = steps.begin(); i != steps.end(); ++i) {
            std::string s(t(StepRange(*i)));
            if(axis.find(s) == axis.end()) {
                std::string z = "0-" + s;
                if(axis.find(z) != axis.end()) {
                    values.push_back(z);
                }
                else {
                    values.push_back(s);
                }
            }
            else {
                values.push_back(s);
            }
        }
    }
    else {
        for (std::vector<std::string>::const_iterator i = steps.begin(); i != steps.end(); ++i) {
            values.push_back(t(StepRange(*i)));
        }
    }
}

void IgnoreHandler::print(std::ostream &out) const {
    out << "IgnoreHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<IgnoreHandler> handler("Ignore");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
