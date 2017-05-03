/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/types/Date.h"

#include "fdb5/LibFdb.h"
#include "fdb5/config/MasterConfig.h"
#include "fdb5/database/Manager.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBInspect.h"
#include "fdb5/tools/ToolRequest.h"

using eckit::Log;
using eckit::Resource;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


FDBInspect::FDBInspect(int argc, char **argv, std::string defaultMinimunKeySet) :
    FDBTool(argc, argv),
    fail_(true) {

    minimumKeySet_ = Resource<std::vector<std::string> >("wipeMinimumKeySet", defaultMinimunKeySet, true);

    if(minimumKeySet_.size() == 0) {
        options_.push_back(new eckit::option::SimpleOption<bool>("all", "Visit all FDB databases"));
    }

    // Be able to turn fail-on-error off
    options_.push_back(
                new eckit::option::SimpleOption<bool>(
                    "ignore-errors",
                    "Ignore errors (report them as warnings) and continue processing wherever possible"));
}


void FDBInspect::execute(const eckit::option::CmdArgs &args) {

    bool all = false;
    args.get("all", all);

    if (all && args.count()) {
        usage(args.tool());
        exit(1);
    }

    bool ignoreErrors;
    args.get("ignore-errors", ignoreErrors);
    if (ignoreErrors) {
        Log::info() << "Errors ignored where possible" << std::endl;
        fail_ = false;
    }

    std::vector<eckit::PathName> paths;

    if (all) {
        Key dbKey;
        Log::debug<LibFdb>() << "KEY =====> " << dbKey << std::endl;
        std::vector<eckit::PathName> dbs = Manager::visitableLocations(dbKey);
        for (std::vector<eckit::PathName>::const_iterator j = dbs.begin(); j != dbs.end(); ++j) {
            Log::debug<LibFdb>() << "Visitable FDB DB location " << *j << std::endl;
            paths.push_back(*j);
        }

        if (dbs.size() == 0) {
            std::stringstream ss;
            ss << "No FDB matches " << dbKey;
            Log::warning() << ss.str() << std::endl;
            if (fail_)
                throw FDBToolException(ss.str(), Here());
        }
    }



    for (size_t i = 0; i < args.count(); ++i) {

        eckit::PathName path(args(i));
        if (path.exists()) {
            paths.push_back(path);
            continue;
        }

        try {

            bool force = false;
            args.get("force", force); //< bypass the check for minimum key set

            ToolRequest req(args(i), force ? std::vector<std::string>() : minimumKeySet_);

            Log::debug<LibFdb>() << "KEY =====> " << req.key() << std::endl;
            std::vector<eckit::PathName> dbs = Manager::visitableLocations(req.key());
            for (std::vector<eckit::PathName>::const_iterator j = dbs.begin(); j != dbs.end(); ++j) {
                paths.push_back(*j);
            }

            if (dbs.size() == 0) {
                std::stringstream ss;
                ss << "No FDB matches " << req.key();
                Log::warning() << ss.str() << std::endl;
                if (fail_)
                    throw FDBToolException(ss.str(), Here());
            }

        } catch (eckit::UserError&) {
            throw;
        } catch (eckit::Exception &e) {
            Log::warning() << e.what() << std::endl;
            paths.push_back(path);
            if (fail_) // Possibly we want a separate catch block like eckit::UserError above
                throw;
        }

    }

    for (std::vector<eckit::PathName>::const_iterator j = paths.begin(); j != paths.end(); ++j) {

        eckit::PathName path = *j;

        if (path.exists()) {

            if (!path.isDir()) {
                path = path.dirName();
            }

            path = path.realName();

        }

        Log::debug<LibFdb>() << "FDBInspect processing path " << path << std::endl;
        process(path , args);

    }
}

bool FDBInspect::fail() const {
    return fail_;
}


void FDBInspect::usage(const std::string &tool) const {
    Log::info() << std::endl
                       << "Usage: " << tool << " [options] [path1|request1] [path2|request2] ..." << std::endl
                       << std::endl
                       << std::endl
                       << "Examples:" << std::endl
                       << "=========" << std::endl << std::endl
                       << tool << " ."
                       << std::endl
                       << tool << " /tmp/fdb/od:0001:oper:20160428:1200:g"
                       << std::endl
                       << tool << " class=od,expver=0001,stream=oper,date=20160428,time=1200,domain=g"
                       << std::endl
                       << std::endl;
    FDBTool::usage(tool);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

