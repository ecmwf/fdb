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

#include "marslib/MarsRequest.h"
#include "marslib/StepRange.h"

#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/TypeStep.h"
#include "fdb5/database/DB.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeStep::TypeStep(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeStep::~TypeStep() {
}


void TypeStep::toKey(std::ostream &out,
                     const std::string &keyword,
                     const std::string &value) const {
    out << StepRange(value);
}

bool TypeStep::match(const std::string&, const std::string& value1, const std::string& value2) const
{
    return (value1 == value2);
}

void TypeStep::getValues(const MarsRequest &request,
                         const std::string &keyword,
                         eckit::StringList &values,
                         const MarsTask &task,
                         const DB *db) const {
    std::vector<std::string> steps;

    request.getValues(keyword, steps);

    eckit::Translator<StepRange, std::string> t;

    values.reserve(steps.size());

    if (db && !steps.empty()) {
        eckit::StringSet axis;
        db->axis(keyword, axis);
        for (std::vector<std::string>::const_iterator i = steps.begin(); i != steps.end(); ++i) {
            std::string s(t(StepRange(*i)));
            if (axis.find(s) == axis.end()) {
                std::string z = "0-" + s;
                if (axis.find(z) != axis.end()) {
                    values.push_back(z);
                } else {
                    values.push_back(s);
                }
            } else {
                values.push_back(s);
            }
        }
    } else {
        for (std::vector<std::string>::const_iterator i = steps.begin(); i != steps.end(); ++i) {
            values.push_back(t(StepRange(*i)));
        }
    }
}

void TypeStep::print(std::ostream &out) const {
    out << "TypeStep[name=" << name_ << "]";
}

static TypeBuilder<TypeStep> type("Step");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
