/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/option/CmdArgs.h"
#include "eckit/types/Date.h"

#include "fdb5/config/MasterConfig.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/TocDB.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBInspect.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


FDBInspect::FDBInspect(int argc, char **argv, const std::vector<std::string>& minimumKeySet):
    FDBTool(argc, argv),
    minimumKeySet_(minimumKeySet) {
    if (minimumKeySet_.size() == 0) {
        options_.push_back(new eckit::option::SimpleOption<bool>("all", "Visit all FDB databases"));
        options_.push_back(new eckit::option::SimpleOption<bool>("", "Visit all FDB databases"));
    }
}


void FDBInspect::execute(const eckit::option::CmdArgs &args) {

    bool all = false;
    args.get("all", all);

    if (all && args.count()) {
        usage(args.tool());
        exit(1);
    }

    std::vector<eckit::PathName> paths;

    if (all) {
        Key dbKey;
        eckit::Log::info() << "KEY =====> " << dbKey << std::endl;
        std::vector<eckit::PathName> dbs = TocDB::databases(dbKey);
        for (std::vector<eckit::PathName>::const_iterator j = dbs.begin(); j != dbs.end(); ++j) {
            paths.push_back(*j);
        }

        if (dbs.size() == 0) {
            eckit::Log::warning() << "No FDB matches " << dbKey << std::endl;
        }
    }



    for (size_t i = 0; i < args.count(); ++i) {

        eckit::PathName path(args(i));
        if (path.exists()) {
            paths.push_back(path);
            continue;
        }

        try {
            Key dbKey(args(i));

            bool force = false;
            args.get("force", force);

            if (!force) {
                for (std::vector<std::string>::const_iterator j = minimumKeySet_.begin(); j != minimumKeySet_.end(); ++j) {
                    if (dbKey.find(*j) == dbKey.end()) {
                        throw eckit::UserError("Please provide a value for '" + (*j) + "'");
                    }
                }
            }

            eckit::Log::info() << "KEY =====> " << dbKey << std::endl;
            std::vector<eckit::PathName> dbs = TocDB::databases(dbKey);
            for (std::vector<eckit::PathName>::const_iterator j = dbs.begin(); j != dbs.end(); ++j) {
                paths.push_back(*j);
            }

            if (dbs.size() == 0) {
                eckit::Log::warning() << "No FDB matches " << dbKey << std::endl;
            }

        } catch (eckit::UserError& e) {
            throw;
        } catch (eckit::Exception &e) {
            eckit::Log::warning() << e.what() << std::endl;
            paths.push_back(path);
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

        // eckit::Log::info() << "PATH =====> " << path << std::endl;
        process( path , args);

    }
}


void FDBInspect::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
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

