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


FDBInspect::FDBInspect(int argc, char **argv):
    FDBTool(argc, argv) {

    options_.push_back(new eckit::option::SimpleOption<std::string>("match", "Provide a partial request,  e.g. --match=expver=0001"));

}


void FDBInspect::execute(const eckit::option::CmdArgs& args) {


    if (args.count() == 0) {

        std::string match = "";
        if (args.get("match", match)) {

            std::vector<eckit::PathName> dbs = TocDB::databases(Key(match));
            for (std::vector<eckit::PathName>::const_iterator i = dbs.begin(); i != dbs.end(); ++i) {
                process(databasePath(*i), args);
            }
        } else {
            usage(args.tool());
        }
    }

    for (size_t i = 0; i < args.count(); ++i) {
        process( databasePath(args(i)) , args);
    }

}


void FDBInspect::usage(const std::string &tool) {
    FDBTool::usage(tool);
}


eckit::PathName FDBInspect::databasePath(const std::string &arg) const {

    eckit::PathName path(arg);
    if (!path.exists()) {
        const Schema& schema = MasterConfig::instance().schema();

        std::ostringstream oss;
        eckit::Date date(0);

        date -= 1; // Yesterday

        oss << "class=od,expver=0001,domain=g,stream=oper,time=1200,date=";
        oss << date.yyyymmdd();
        oss << "," << arg;

        try {
            Key dbKey(oss.str());
            Key result;

            if (schema.expandFirstLevel(dbKey, result)) {
                path = TocDB::directory(result);
                // Log::info() << arg << " => " << dbKey << " => " << result << " => " << path << std::endl;
            }
        } catch (eckit::Exception& e) {

        }
    }

    if (path.exists()) {

        if (!path.isDir()) {
            path = path.dirName();
        }

        path = path.realName();
    }



    return path;

}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

