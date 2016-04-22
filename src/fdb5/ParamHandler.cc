/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/KeywordType.h"
#include "fdb5/ParamHandler.h"
#include "fdb5/DB.h"

#include "marslib/MarsParam.h"
#include "marslib/MarsTask.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ParamHandler::ParamHandler(const std::string& name, const std::string& type) :
    KeywordHandler(name, type)
{
}

ParamHandler::~ParamHandler()
{
}

void ParamHandler::getValues(const MarsRequest& request,
                             const std::string& keyword,
                             eckit::StringList& values,
                             const MarsTask& task,
                             const DB* db) const {
    ASSERT(db);

    eckit::StringSet ax;

    db->axis(keyword, ax);

    eckit::StringList us;

    KeywordHandler::getValues(request, keyword, us, task, db);

    std::vector<Param> user;
    std::copy(us.begin(), us.end(), std::back_inserter(user));

    std::vector<Param> axis;
    std::copy(ax.begin(), ax.end(), std::back_inserter(axis));
    std::sort(axis.begin(), axis.end());

    bool windConvertion = false;
    MarsParam::substitute(request, user, axis, windConvertion);

    std::copy(user.begin(), user.end(), std::back_inserter(values));

    Log::info() << "ParamHandler before: " << us << std::endl;
    Log::info() << "              after: " << values << std::endl;
    Log::info() << "               wind: " << (windConvertion ? "true" : "false") << std::endl;
    // Log::info() << "               user: " << user << std::endl;
    Log::info() << "               axis: " << ax << std::endl;

    if(windConvertion) {
        task.notifyWinds();
    }
}

void ParamHandler::print(std::ostream &out) const
{
    out << "ParamHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<ParamHandler> handler("Param");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
